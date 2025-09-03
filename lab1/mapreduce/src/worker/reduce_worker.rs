use std::io::Read;
use tonic::{Request, Response, Status, async_trait};

use super::{messages};

use messages::map_worker_client::MapWorkerClient;

#[async_trait]
impl<Key, Value> messages::reduce_worker_server::ReduceWorker for crate::worker::ReduceWorker<Key, Value>
where
    Key: Send + Sync + Clone + prost::Message + Default + 'static,
    Value: Send + Sync + prost::Message + Default + 'static,
{
    async fn run_reduce(
        &self,
        request: Request<messages::ReduceRequest>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        let request = request.into_inner();
        for connection in request.map_output_locations {
            let Ok(mut client) = MapWorkerClient::connect(connection.clone()).await else {
                tracing::error!("Failed to connect to worker: {}", connection);
                continue;
            };
            let fetch_request = Request::new(messages::FetchPartitionRequest {
                partition_id: request.partition_id,
            });
            let Ok(response) = client.fetch_partition(fetch_request).await else {
                tracing::error!("Failed to fetch partition from worker: {}", connection);
                continue;
            };
            let kv_pairs = response.into_inner().values;
            for pair in kv_pairs {
                let key = Key::decode(pair.key.as_slice())
                    .map_err(|_| Status::internal("Failed to decode key"))?;
                let value = Value::decode(pair.value.as_slice())
                    .map_err(|_| Status::internal("Failed to decode value"))?;
                // Reduce worker stores reduced values in local cache
                self.local_cache
                    .entry(request.partition_id.clone())
                    .or_insert_with(Vec::new)
                    .push((key, value));
            }
        }

        tracing::info!("Reduced partition: {}", request.partition_id);
        Ok(Response::new(messages::BasicResponse { success: true }))
    }

    async fn halt(
        &self,
        request: Request<messages::Empty>,
    ) -> Result<Response<messages::BasicResponse>, Status> {
        // todo!();
        Ok(Response::new(messages::BasicResponse { success: true }))
    }
}
