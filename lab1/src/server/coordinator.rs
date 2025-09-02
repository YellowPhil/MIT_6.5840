use std::{collections::VecDeque, io::Read, sync::Arc};

use dashmap::DashMap;
use eyre::{Result, WrapErr};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_stream::iter;
use tonic::Request;

mod messages {
    tonic::include_proto!("messages");
}

const BATCH_SIZE: usize = 1000;

type WorkerId = usize;
type BucketId = usize;

pub struct Coordinator {
    mappers: Vec<WorkerInfo>,
    reducers: Vec<WorkerInfo>,
    map_reducer: DashMap<WorkerId, BucketId>,
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
            map_reducer: DashMap::new(),
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

        let mut work_queue: VecDeque<Vec<(Key, Value)>> = VecDeque::new();
        for i in 0..self.mappers.len() {
            let value_slice = values[i * iter_step..(i + 1) * iter_step].to_vec();
            work_queue.push_back(value_slice);
        }

        let mut join_set = JoinSet::new();
        let work_queue = Arc::new(Mutex::new(work_queue));

        for i in 0..self.mappers.len() {
            let mapper_connection = self.mappers[i].connection.clone();
            let work_queue_clone = work_queue.clone();

            join_set.spawn(async move {
                Self::worker_loop(mapper_connection, work_queue_clone, i).await
            });
        }

        join_set.join_all().await;
        Ok(())
    }

    async fn worker_loop<Key, Value>(
        connection: messages::worker_client::WorkerClient<tonic::transport::Channel>,
        work_queue: Arc<Mutex<VecDeque<Vec<(Key, Value)>>>>,
        worker_id: usize,
    ) -> Result<()>
    where
        Key: Into<Box<dyn Read>> + Default + prost::Message + Send + 'static + Clone,
        Value: Default + prost::Message + Send + 'static + Clone,
    {
        loop {
            let batch = {
                let mut queue = work_queue.lock().await;
                queue.pop_front()
            };

            let Some(values) = batch else {
                break;
            };

            match Self::process_mapper_batch(connection.clone(), values.clone(), worker_id).await {
                Ok(()) => {
                    tracing::info!("Worker {} successfully processed batch", worker_id);
                }
                Err(e) => {
                    tracing::warn!(
                        "Worker {} failed to process batch: {}. Requeueing work.",
                        worker_id,
                        e
                    );
                    let mut queue = work_queue.lock().await;
                    queue.push_back(values);
                    break;
                }
            }
        }
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
                values: chunk
                    .iter()
                    .map(|(key, value)| messages::Kv {
                        key: key.encode_to_vec(),
                        value: value.encode_to_vec(),
                    })
                    .collect(),
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
