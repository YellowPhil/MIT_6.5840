use std::io::Read;

use tokio::task::JoinSet;
use tonic::Request;
use tokio_stream::iter;

use eyre::{Result, WrapErr};
mod messages {
    tonic::include_proto!("messages");
}

const BATCH_SIZE: usize = 1000;

pub struct Coordinator {
    mappers: Vec<WorkerInfo>,
    reducers: Vec<WorkerInfo>,
}

struct WorkerInfo {
    id: u32,
    connection: messages::worker_client::WorkerClient<tonic::transport::Channel>,
    location: String,
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            mappers: Vec::new(),
            reducers: Vec::new(),
        }
    }
    pub async fn add_mapper(&mut self, id: u32, location: String) -> Result<()> {
        tracing::info!("Adding mapper: {}", location);
        let connection = messages::worker_client::WorkerClient::connect(location.clone())
            .await
            .wrap_err("Failed to connect to mapper")?;

        self.mappers.push(WorkerInfo {
            id,
            connection,
            location,
        });
        Ok(())
    }
    pub async fn add_reducer(&mut self, id: u32, location: String) -> Result<()> {
        tracing::info!("Adding reducer: {}", location);
        let connection = messages::worker_client::WorkerClient::connect(location.clone())
            .await
            .wrap_err("Failed to connect to reducer")?;
        self.reducers.push(WorkerInfo {
            id,
            connection,
            location,
        });
        Ok(())
    }
    pub async fn map<Key, Value>(&mut self, values: Vec<(Key, Value)>) -> Result<()>
    where
        Key: Into<Box<dyn Read>> + Default + prost::Message + Send + 'static + Clone,
        Value: Default + prost::Message + Send + 'static + Clone,
    {
        let iter_step = values.len() / self.mappers.len();
        
        let mut join_set = JoinSet::new();
        
        for i in 0..self.mappers.len() {
            let mapper_connection = self.mappers[i].connection.clone();
            let value_slice = values[i * iter_step..(i + 1) * iter_step].to_vec();
            
            join_set.spawn(async move {
                Self::process_mapper_batch(mapper_connection, value_slice, i).await
            });
        }
        join_set.join_all().await;
        Ok(())
    }
    
    async fn process_mapper_batch<Key, Value>(
        mut connection: messages::worker_client::WorkerClient<tonic::transport::Channel>,
        values: Vec<(Key, Value)>,
        mapper_id: usize,
    ) -> Result<()>
    where
        Key: Into<Box<dyn Read>> + Default + prost::Message + Send + 'static + Clone,
        Value: Default + prost::Message + Send + 'static + Clone,
    {
        let mut batches = Vec::new();
        for chunk in values.chunks(BATCH_SIZE) {
            let batch = messages::MapRequest {
                values: chunk.iter().map(|(key, value)| messages::Kv {
                    key: key.encode_to_vec(),
                    value: value.encode_to_vec(),
                }).collect(),
            };
            batches.push(batch);
        }
        let stream = iter(batches.into_iter());
        
        let request = Request::new(stream);
        connection.run_map(request).await?;
        tracing::info!("Mapped partition: {}", mapper_id);
        Ok(())
    }
}

fn main() {}
