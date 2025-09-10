use std::{collections::VecDeque, io::Read, sync::Arc};

use eyre::{Result, WrapErr};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_stream::iter;
use tonic::{Request, transport::Channel};

mod messages {
    tonic::include_proto!("messages");
}

const BATCH_SIZE: usize = 1000;

pub struct Coordinator {
    mappers: Vec<WorkerInfo>,
    reducers: Vec<WorkerInfo>,
}

enum WorkerType {
    Mapper(messages::map_worker_client::MapWorkerClient<Channel>),
    Reducer(messages::reduce_worker_client::ReduceWorkerClient<Channel>),
}

struct WorkerInfo {
    id: u32,
    location: String,
    worker_type: WorkerType,
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
        let connection = messages::map_worker_client::MapWorkerClient::connect(location.clone())
            .await
            .wrap_err("Failed to connect to mapper")?;

        self.mappers.push(WorkerInfo {
            id,
            location,
            worker_type: WorkerType::Mapper(connection),
        });
        Ok(())
    }

    pub async fn add_reducer(&mut self, id: u32, location: String) -> Result<()> {
        tracing::info!("Adding reducer: {}", location);
        let connection =
            messages::reduce_worker_client::ReduceWorkerClient::connect(location.clone())
                .await
                .wrap_err("Failed to connect to reducer")?;
        self.reducers.push(WorkerInfo {
            id,
            location,
            worker_type: WorkerType::Reducer(connection),
        });
        for mapper in &mut self.mappers {
            match &mut mapper.worker_type {
                WorkerType::Mapper(connection) => {
                    connection
                        .notify_new_reducer(Request::new(messages::Empty {}))
                        .await?;
                }
                WorkerType::Reducer(_) => continue,
            }
        }
        Ok(())
    }
    pub async fn map<Key, Value>(&mut self, values: Vec<(Key, Value)>) -> Result<()>
    where
        Key: Into<Box<dyn Read>> + Default + prost::Message + Send + 'static + Clone,
        Value: Default + prost::Message + Send + 'static + Clone,
    {
        let iter_step = if values.len() < self.mappers.len() {
            1
        } else {
            values.len() / self.mappers.len()
        };

        let mut work_queue: VecDeque<Vec<(Key, Value)>> = VecDeque::new();
        for i in 0..self.mappers.len() {
            let value_slice = values[i * iter_step..(i + 1) * iter_step].to_vec();
            work_queue.push_back(value_slice);
        }

        let mut join_set = JoinSet::new();
        let work_queue = Arc::new(Mutex::new(work_queue));

        for i in 0..self.mappers.len() {
            let work_queue_clone = work_queue.clone();
            let connection = match &self.mappers[i].worker_type {
                WorkerType::Mapper(connection) => connection.clone(),
                WorkerType::Reducer(_) => {
                    return Err(eyre::eyre!("Reducer cannot process map requests"));
                }
            };

            join_set.spawn(async move { Self::worker_loop(connection, work_queue_clone, i).await });
        }

        join_set.join_all().await;
        self.assign_reduce().await?;
        Ok(())
    }

    async fn worker_loop<Key, Value>(
        connection: messages::map_worker_client::MapWorkerClient<Channel>,
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
            tracing::debug!("Worker {} got batch with size: {}", worker_id, values.len());

            match Self::process_mapper_batch(connection.clone(), &values, worker_id).await {
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
        mut connection: messages::map_worker_client::MapWorkerClient<Channel>,
        values: &[(Key, Value)],
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
            tracing::debug!("Sending chunk of size {} to mapper: {}", chunk.len(), mapper_id);
            batches.push(batch);
        }
        let stream = iter(batches.into_iter());

        let request = Request::new(stream);
        connection.run_map(request).await?;
        tracing::info!("Mapped partition: {}", mapper_id);
        Ok(())
    }

    async fn assign_reduce(&mut self) -> Result<()> {
        let mappers = self
            .mappers
            .iter()
            .map(|mapper| mapper.location.clone())
            .collect::<Vec<_>>();

        for (i, reducer) in self.reducers.iter().enumerate() {
            let mut connection = match &reducer.worker_type {
                WorkerType::Reducer(connection) => connection.clone(),
                WorkerType::Mapper(_) => {
                    return Err(eyre::eyre!("Mapper cannot process reduce requests"));
                }
            };
            let request = Request::new(messages::ReduceRequest {
                partition_id: i as u32,
                map_output_locations: mappers.clone(),
            });
            match connection.run_reduce(request).await {
                Ok(_) => {
                    tracing::info!("Reducer {} successfully processed partition", i);
                }
                Err(e) => {
                    tracing::warn!(
                        "Reducer {} failed to process partition: {}. Requeueing work.",
                        reducer.id,
                        e
                    );
                }
            }
        }
        Ok(())
    }
}
