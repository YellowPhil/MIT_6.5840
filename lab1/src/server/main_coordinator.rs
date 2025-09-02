mod coordinator;

use coordinator::Coordinator;
use tonic::{Request, Response, Status};

mod messages {
    tonic::include_proto!("messages");
}


#[tonic::async_trait]
impl messages::coordinator_server::Coordinator for Coordinator {
    async fn assign_reduce(&self, request: Request<messages::ReduceRequest>) -> Result<Response<messages::BasicResponse>, Status> {
        todo!();
    }
    async fn get_reduce_task(&self, request: Request<messages::Empty>) -> Result<Response<messages::ReduceRequest>, Status> {
        todo!();
    }
}

fn main() {}