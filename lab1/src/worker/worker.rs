use std::pin::Pin;

use dashmap::DashMap;
use tokio_stream::{StreamExt};
use tonic::{Request, Response, Status, Streaming};

mod messages {
    tonic::include_proto!("messages");
}

pub struct Worker<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Sized + Send + Sync,
    Value: Sized + Send + Sync,
{
    id: u32,
    channel: tonic::transport::Channel,
    local_map: DashMap<Key, Vec<Value>>,
    map: MapFn<Key, Value>,
    reduce: ReduceFn<Key, Value>,
}

pub type MapFn<Key, Value> = Box<dyn Fn(Key, Value) -> Vec<(Key, Value)> + Send + Sync>;
pub type ReduceFn<Key, Value> = Box<dyn Fn(Vec<(Key, Value)>) -> Value + Send + Sync>;

impl<Key, Value> Worker<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Sized + Send + Sync,
    Value: Sized + Send + Sync,
{
    pub fn new(
        id: u32,
        channel: tonic::transport::Channel,
        map: MapFn<Key, Value>,
        reduce: ReduceFn<Key, Value>,
    ) -> Self {
        Self {
            id,
            channel,
            local_map: DashMap::new(),
            map,
            reduce,
        }
    }
}

#[tonic::async_trait]
impl<Key, Value> messages::worker_server::Worker for Worker<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Send + Sync + 'static + prost::Message + Default,
    Value: Send + Sync + 'static + prost::Message + Default,
{
    async fn map(
        &self,
        request: Request<Streaming<messages::MapRequest>>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        let mut stream = request.into_inner();

        while let Some(request) = stream.next().await {
            let request = request?;
            let key = Key::decode(request.key.as_slice())
                .map_err(|_| Status::invalid_argument("Invalid key"))?;
            let value = Value::decode(request.value.as_slice())
                .map_err(|_| Status::invalid_argument("Invalid value"))?;

            for (key, value) in self.map(key, value) {
                self.local_map
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(value);
            }
        }
        Ok(Response::new(messages::BasicResponse { success: true }))
    }

    async fn fetch_partition(
        &self,
        request: Request<messages::FetchPartitionRequest>,
    ) -> Result<Response<messages::FetchPartitionResponse>, Status> {
        let request = request.into_inner();
        let key = Key::decode(request.key.as_slice())
            .map_err(|_| Status::invalid_argument("Invalid key"))?;

        let values = self
            .local_map
            .get(&key)
            .ok_or(Status::not_found("Key not found"))?
            .iter()
            .map(|value| value.encode_to_vec())
            .collect::<Vec<_>>();

        Ok(Response::new(messages::FetchPartitionResponse { values }))
    }
    async fn run_reduce(
        &self,
        request: Request<messages::ReduceTask>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        todo!();
        Ok(Response::new(messages::BasicResponse { success: true }))
    }
    async fn halt(
        &self,
        request: Request<messages::Empty>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        todo!();
        Ok(Response::new(messages::BasicResponse { success: true }))
    }
}

impl<Key, Value> Worker<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Sized + Send + Sync,
    Value: Sized + Send + Sync,
{
    fn map(&self, key: Key, value: Value) -> Vec<(Key, Value)> {
        (self.map)(key, value)
    }
}
