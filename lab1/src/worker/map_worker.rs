use std::io::Read;

use murmur3::murmur3_32;
mod messages {
    tonic::include_proto!("messages");
}

use super::Worker;

pub trait MapWorker<Key, Value>
where
    Key: Into<Box<dyn Read>>,
{
    fn map(&self, key: Key, value: Value) -> eyre::Result<()>;
}

impl<Key, Value> MapWorker<Key, Value> for Worker<Key, Value>
where
    Key: Into<Box<dyn Read>> + Send + Sync + Clone,
    Value: Send + Sync,
{
    fn map(&self, key: Key, value: Value) -> eyre::Result<()> {
        let bucket_id = murmur3_32(&mut key.clone().into(), 0)
            .map_err(|_| eyre::eyre!("Failed to hash key"))?;
        for (key, value) in (self.map_fn)(key, value) {
            self.local_cache
                .entry(bucket_id)
                .or_insert_with(Vec::new)
                .push((key, value));
        }
        Ok(())
    }
}
