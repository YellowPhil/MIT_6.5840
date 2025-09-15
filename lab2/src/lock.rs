use std::time::Duration;

use eyre::{Result, WrapErr};
use kv::client::{Client, ClientError};

pub const LOCK_KEY: &str = "lock";
pub const UNLOCKED_VALUE: &str = "unlocked";
pub const LOCKED_VALUE: &str = "locked";

#[derive(Debug, PartialEq)]
enum LockState {
    Unlocked,
    Locked,
}
struct DistributedLock {
    state: LockState,
    client: Client,
    sleep_duration: Duration,
    locked_value: String,
}

impl DistributedLock {
    /// Creates a new DistributedLock instance.
    ///
    /// **Warning**: Consider setting a randomized sleep_duration to prevent the thundering herd problem.
    /// When multiple clients are waiting for the same lock, they may all wake up simultaneously
    /// and compete for the lock, causing unnecessary load on the server. Random sleep intervals
    /// help distribute the retry attempts more evenly over time.
    fn new(connection_string: &str, id: usize) -> Self {
        Self {
            state: LockState::Unlocked,
            client: Client::new(connection_string),
            sleep_duration: Duration::from_secs(1),
            locked_value: format!("locked_{}", id),
        }
    }

    /// Acquires the distributed lock.
    ///
    /// This method attempts to acquire a distributed lock by checking the current lock state
    /// and attempting to set it to this client's unique locked value. The method will retry
    /// indefinitely until the lock is successfully acquired.
    ///
    /// ## Behavior
    ///
    /// 1. **Check existing lock state**: First checks if a lock key exists in the store
    /// 2. **Handle missing key**: If no lock key exists, creates one with an unlocked value
    /// 3. **Already locked by this client**: If the lock is already held by this client, returns immediately
    /// 4. **Lock held by another client**: If locked by another client, waits and retries
    /// 5. **Attempt to acquire**: Tries to atomically set the lock to this client's value
    /// 6. **Handle conflicts**: Retries on version mismatches or value changes
    ///
    /// ## Error Handling
    ///
    /// - Returns an error if there are unexpected client communication failures
    /// - Automatically retries on version conflicts (optimistic concurrency control)
    /// - Will retry indefinitely until the lock is acquired or an unrecoverable error occurs
    ///
    /// ## Examples
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # async fn example() -> eyre::Result<()> {
    /// let mut lock = DistributedLock::new("http://localhost:50051", 1);
    ///
    /// // Acquire the lock (will block until available)
    /// lock.lock().await?;
    ///
    /// // Critical section - only one client can execute this at a time
    /// println!("Lock acquired, performing critical work...");
    ///
    /// // Don't forget to unlock!
    /// lock.unlock().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Notes
    ///
    /// - This method modifies the internal state to `LockState::Locked` upon success
    /// - The lock does NOT automatically release when the object is dropped
    /// - You must manually call [`unlock`](Self::unlock) to release the lock
    /// - Consider the thundering herd problem when multiple clients compete for the same lock
    async fn lock(&mut self) -> Result<()> {
        while self.state == LockState::Unlocked {
            let lock_state = match self.client.get(LOCK_KEY).await {
                Ok(value) => Ok(value),
                Err(ClientError::RequestError(status))
                    if status.code() == tonic::Code::NotFound =>
                {
                    Ok({
                        match self.client.put(LOCK_KEY, &self.locked_value).await {
                            Ok(_) => Ok(self.locked_value.clone()),
                            Err(ClientError::VersionMismatch(_)) => continue,
                            Err(e) => Err(e),
                        }?
                    })
                }
                Err(e) => Err(e),
            }?;

            if lock_state == self.locked_value {
                self.state = LockState::Locked;
                return Ok(());
            } else if lock_state != UNLOCKED_VALUE {
                tokio::time::sleep(self.sleep_duration).await;
                continue;
            }

            let client_put_result = self.client.put(LOCK_KEY, &self.locked_value).await;
            if client_put_result.is_ok() {
                self.state = LockState::Locked;
                return Ok(());
            }
            let (Err(ClientError::VersionMismatch(_)) | Err(ClientError::ValueMayBeChanged)) =
                client_put_result
            else {
                return Err(eyre::eyre!("Failed to lock. Consider retrying."));
            };
        }
        Ok(())
    }

    /// Releases the distributed lock.
    ///
    /// **Warning**: This lock does NOT automatically unlock when dropped! You must manually
    /// call this method to release the lock. Please do NOT forget to release the lock when
    /// you're done with the critical section, as failing to do so will cause other clients
    /// to wait indefinitely.
    async fn unlock(&mut self) -> Result<()> {
        if self.state == LockState::Unlocked {
            return Ok(());
        }
        let lock_value = self
            .client
            .get(LOCK_KEY)
            .await
            .wrap_err("Failed to get lock value. Maybe the server is not running.")?;
        if lock_value == self.locked_value {
            self.client
                .put(LOCK_KEY, UNLOCKED_VALUE)
                .await
                .wrap_err("Failed to unlock. Consider retrying.")?;
            self.state = LockState::Unlocked;
            return Ok(());
        }
        Err(eyre::eyre!(
            "Failed to unlock. The lock is being held by another client."
        ))
    }
}

#[cfg(test)]
mod test {
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use tonic::transport::Server;

    use super::*;
    static ADDRESS: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

    async fn spawn_server() -> String {
        let addr = "127.0.0.1:50055";
        let server = Server::builder().add_service(
            kv::storage_rpc::storage_server::StorageServer::new(kv::storage::KvStorage::new()),
        );
        tokio::spawn(async move {
            let _ = server.serve(addr.parse().unwrap()).await.unwrap();
        });
        tokio::time::sleep(Duration::from_secs(1)).await;

        format!("http://{addr}")
    }

    async fn random_id() -> usize {
        rand::rng().random_range(1..1000000)
    }

    #[tokio::test]
    async fn test_lock_unlock() {
        let addr = ADDRESS.get_or_init(spawn_server).await;
        let mut lock = DistributedLock::new(addr, random_id().await);
        for _ in 0..100 {
            lock.lock().await.unwrap();
            assert_eq!(lock.state, LockState::Locked);
            lock.unlock().await.unwrap();
            assert_eq!(lock.state, LockState::Unlocked);
        }
    }

    #[tokio::test]
    async fn test_lock_unlock_concurrent() {
        let addr = ADDRESS.get_or_init(spawn_server).await;
        let locks = futures::future::join_all(
            (0..10).map(|_| async move { DistributedLock::new(addr, random_id().await) }),
        ).await;
        let mut join_set = tokio::task::JoinSet::new();
        for mut lock in locks.into_iter() {
            join_set.spawn(async move {
                let mut rng = SmallRng::from_os_rng();
                for _ in 0..5 {
                    lock.lock().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(rng.random_range(0..10))).await;
                    lock.unlock().await.unwrap();
                }
            });
        }
        join_set.join_all().await;
    }
}

#[tokio::main]
async fn main() { }
