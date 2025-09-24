use std::io::Read;
use tokio_stream::StreamExt;

use murmur3::murmur3_32;
use tonic::{Request, Response, Status, Streaming};

use super::{MapWorkerImpl};
use crate::messages;

#[tonic::async_trait]
impl<Key, Value> messages::map_worker_server::MapWorker for MapWorkerImpl<Key, Value>
where
    Key: Send + Clone + 'static + prost::Message + Default + Into<Box<dyn Read>>,
    Value: Send + 'static + prost::Message + Default,
{
    async fn run_map(
        &self,
        request: Request<Streaming<messages::MapRequest>>,
    ) -> Result<Response<messages::BasicResponse>, Status> {

        if *self.reducers_amount.lock().unwrap() == 0 {
            tracing::warn!("No reducers are connected, skipping map request");
            return Err(Status::invalid_argument("No reducers are connected"));
        }

        let mut stream: Streaming<messages::MapRequest> = request.into_inner();
        tracing::info!("Received map request into worker: {}", self.id);

        while let Some(request) = stream.next().await {
            let request = request?;
            tracing::debug!("Received map request chunk of size: {}", request.values.len());
            for kv in request.values {
                let key = Key::decode(kv.key.as_slice())
                    .map_err(|_| Status::invalid_argument("Invalid key"))?;
                let value = Value::decode(kv.value.as_slice())
                    .map_err(|_| Status::invalid_argument("Invalid value"))?;

                let bucket_id = murmur3_32(&mut key.clone().into(), 0)
                    .map_err(|_| Status::internal("Failed to hash key"))?;
                for (key, value) in (self.map_fn)(key, value) {
                    let reducers_amount = { *self.reducers_amount.lock().unwrap() };
                    // Map worker stores mapped values in local cache at HASH % REDUCERS_AMOUNT index
                    tracing::debug!("Mapping value to bucket: {}", bucket_id % reducers_amount as u32);
                    self.local_cache
                        .entry(bucket_id % reducers_amount as u32)
                        .or_insert_with(Vec::new)
                        .push((key, value));
                }
            }
        }
        tracing::info!("Mapping completed for worker: {}", self.id);
        Ok(Response::new(messages::BasicResponse { success: true }))
    }

    async fn fetch_partition(
        &self,
        request: Request<messages::FetchPartitionRequest>,
    ) -> Result<Response<messages::FetchPartitionResponse>, Status> {
        let request = request.into_inner();
        tracing::info!("Fetching partition: {} from worker: {}", request.partition_id, self.id);
        let values = self
            .local_cache
            .get(&request.partition_id)
            .map(|cache_values| {
                cache_values
                    .iter()
                    .map(|(key, value)| messages::Kv {
                        key: key.encode_to_vec(),
                        value: value.encode_to_vec(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(Vec::new);
        tracing::info!("Removing partition: {} from worker: {}", request.partition_id, self.id);
        self.local_cache.remove(&request.partition_id);

        Ok(Response::new(messages::FetchPartitionResponse { values }))
    }
    async fn notify_new_reducer(
        &self,
        request: Request<messages::Empty>,
    ) -> Result<Response<messages::Empty>, Status> {
        tracing::info!("Notifying about a new reducer to worker: {}", self.id);
        {
            *self.reducers_amount.lock().unwrap() += 1;
        }
        Ok(Response::new(messages::Empty {}))
    }
}
