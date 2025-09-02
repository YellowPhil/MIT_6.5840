use dashmap::DashMap;

pub mod map_worker;
pub mod reduce_worker;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use std::io::Read;

mod messages {
    tonic::include_proto!("messages");
}

use map_worker::MapWorker;
use reduce_worker::ReduceWorker;

pub struct Worker<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    id: u32,
    // TODO: this design choice may be suboptimal, need some storage abstraction
    local_cache: DashMap<BucketId, Vec<(Key, Value)>>,
    map_fn: MapFn<Key, Value>,
    reduce_fn: ReduceFn<Key, Value>,
}

pub type MapFn<Key, Value> = Box<dyn Fn(Key, Value) -> Vec<(Key, Value)> + Send + Sync>;
pub type ReduceFn<Key, Value> = Box<dyn Fn(Key, Vec<Value>) -> Vec<Value> + Send + Sync>;
pub type BucketId = u32;

impl<Key, Value> Worker<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    pub fn new(id: u32, map_fn: MapFn<Key, Value>, reduce_fn: ReduceFn<Key, Value>) -> Self {
        Self {
            id,
            local_cache: DashMap::new(),
            map_fn,
            reduce_fn,
        }
    }
}

#[tonic::async_trait]
impl<Key, Value> messages::worker_server::Worker for Worker<Key, Value>
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
            
            // Process each batch of KV pairs in the MapRequest
            for kv in request.values {
                let key = Key::decode(kv.key.as_slice())
                    .map_err(|_| Status::invalid_argument("Invalid key"))?;
                let value = Value::decode(kv.value.as_slice())
                    .map_err(|_| Status::invalid_argument("Invalid value"))?;

                self.map(key, value)
                    .map_err(|_| Status::internal("Failed to map"))?;
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

    async fn halt(
        &self,
        _request: Request<messages::Empty>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        //TODO: Implement graceful shutdown
        Ok(Response::new(messages::BasicResponse { success: true }))
    }
    async fn run_reduce(
        &self,
        request: Request<messages::ReduceRequest>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        let request = request.into_inner();
        self.reduce(request.partition_id, request.map_output_locations)
            .await
            .map_err(|_| Status::internal("Failed to reduce"))?;
        tracing::info!("Reduced partition: {}", request.partition_id);
        Ok(Response::new(messages::BasicResponse { success: true }))
    }
}

fn main() {}
