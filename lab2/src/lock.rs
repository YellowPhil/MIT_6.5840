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
    id: usize,
    state: LockState,
    connection_string: String,
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
            id,
            state: LockState::Unlocked,
            connection_string: connection_string.to_string(),
            client: Client::new(connection_string),
            sleep_duration: Duration::from_secs(1),
            locked_value: format!("locked_{}", id),
        }
    }

    async fn lock(&mut self) -> Result<()> {
        while self.state == LockState::Unlocked {
            let lock_state = match self.client.get(LOCK_KEY).await {
                Ok(value) => Ok(value),
                Err(ClientError::RequestError(status)) if status.code() == tonic::Code::NotFound => {
                    self.client.put(LOCK_KEY, UNLOCKED_VALUE).await?;
                    Ok(UNLOCKED_VALUE.to_string())
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
            let (Err(ClientError::VersionMismatch(_)) | Err(ClientError::ValueMayBeChanged)) = client_put_result else {
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
        let lock_value = self.client.get(LOCK_KEY).await.wrap_err("Failed to get lock value. Maybe the server is not running.")?;
        if lock_value == self.locked_value {
            self.client.put(LOCK_KEY, UNLOCKED_VALUE).await.wrap_err("Failed to unlock. Consider retrying.")?;
            self.state = LockState::Unlocked;
            return Ok(());
        }
        Err(eyre::eyre!("Failed to unlock. The lock is being held by another client."))
    }
}

#[tokio::main]
async fn main() {
    let mut lock = DistributedLock::new("http://127.0.0.1:50051", 1);
    lock.lock().await.unwrap();
}