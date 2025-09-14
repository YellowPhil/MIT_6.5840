use kv::client::Client;
use eyre::{WrapErr, Result};


pub const LOCK_KEY: &str = "lock";

enum LockState {
    Unlocked,
    Locked(usize)
}
struct DistributedLock {
    id: usize,
    state: LockState,
    connection_string: String,
    client: Client
}

impl DistributedLock {
    fn new(connection_string: &str, id: usize) -> Self {
        Self {
            id,
            state: LockState::Unlocked,
            connection_string: connection_string.to_string(),
            client: Client::new(connection_string),
        }
    }

    async fn lock(&self) -> Result<()> {
        // self.client.put(LOCK_KEY, self.id.to_string().as_str()).await.wrap_err("Failed to lock")?;
        // self.state = LockState::Locked(self.id);
        Ok(())
    }

    async fn unlock(&self) -> Result<()> {
        // self.client.put(LOCK_KEY, "0").await.wrap_err("Failed to unlock")?;
        // self.state = LockState::Unlocked;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // let lock = DistributedLock::new();
    lock.lock().await;
}