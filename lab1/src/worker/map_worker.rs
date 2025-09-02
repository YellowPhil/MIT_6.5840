use std::io::Read;
use tokio_stream::StreamExt;

use murmur3::murmur3_32;
use tonic::{Request, Response, Status, Streaming};

mod messages {
    tonic::include_proto!("messages");
}

use super::Worker;

#[tonic::async_trait]
impl<Key, Value> messages::map_worker_server::MapWorker for Worker<Key, Value>
where
    Key: Send + Sync + 'static + prost::Message + Default,
    Key: Into<Box<dyn Read>> + Clone,
    Value: Send + Sync + 'static + prost::Message + Default,
{
    async fn run_map(
        &self,
        request: Request<Streaming<messages::MapRequest>>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        let mut stream: Streaming<messages::MapRequest> = request.into_inner();

        while let Some(request) = stream.next().await {
            let request = request?;
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
                    self.local_cache
                        .entry(bucket_id % reducers_amount as u32)
                        .or_insert_with(Vec::new)
                        .push((key, value));
                }
            }
        }
        Ok(Response::new(messages::BasicResponse { success: true }))
    }

    async fn fetch_partition(
        &self,
        request: Request<messages::FetchPartitionRequest>,
    ) -> Result<Response<messages::FetchPartitionResponse>, Status> {
        let request = request.into_inner();
        let values = self
            .local_cache
            .get(&request.partition_id)
            .ok_or(Status::not_found("Key not found"))?
            .iter()
            .map(|(key, value)| messages::Kv {
                key: key.encode_to_vec(),
                value: value.encode_to_vec(),
            })
            .collect::<Vec<_>>();

        Ok(Response::new(messages::FetchPartitionResponse { values }))
    }
    async fn notify_new_reducer(
        &self,
        request: Request<messages::Empty>,
    ) -> Result<Response<messages::Empty>, Status> {
        {
            *self.reducers_amount.lock().unwrap() += 1;
        }
        Ok(Response::new(messages::Empty {}))
    }
}
