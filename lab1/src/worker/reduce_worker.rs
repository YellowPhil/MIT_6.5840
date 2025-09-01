use std::io::Read;

use tonic::{Request, async_trait};

mod messages {
    tonic::include_proto!("messages");
}

use crate::BucketId;

use super::Worker;
use messages::worker_client::WorkerClient;

#[async_trait]
pub trait ReduceWorker<Key, Value>
where
    Key: Into<Box<dyn Read>> + Clone + Send,
{
    async fn reduce(&self, partition_id: BucketId, connections: Vec<String>) -> eyre::Result<()>;
}

#[async_trait]
impl<Key, Value> ReduceWorker<Key, Value> for Worker<Key, Value>
where
    Key: Send + Sync + Clone + Into<Box<dyn Read>> + prost::Message + Default,
    Value: Send + Sync + prost::Message + Default,
{
    async fn reduce(&self, partition_id: BucketId, connections: Vec<String>) -> eyre::Result<()> {
        for connection in connections {
            let Ok(mut client) = WorkerClient::connect(connection.clone()).await else {
                tracing::error!("Failed to connect to worker: {}", connection);
                continue;
            };
            let request = Request::new(messages::FetchPartitionRequest {
                partition_id: partition_id,
            });
            let Ok(response) = client.fetch_partition(request).await else {
                tracing::error!("Failed to fetch partition from worker: {}", connection);
                continue;
            };
            let kv_pairs = response.into_inner().values;
            for pair in kv_pairs {
                let key = Key::decode(pair.key.as_slice())
                    .map_err(|_| eyre::eyre!("Failed to decode key"))?;
                let value = Value::decode(pair.value.as_slice())
                    .map_err(|_| eyre::eyre!("Failed to decode value"))?;
                self.local_cache
                    .entry(partition_id)
                    .or_insert_with(Vec::new)
                    .push((key, value));
            }
        }
        Ok(())
    }
}