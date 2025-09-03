pub mod coordinator;
pub mod worker;

pub mod messages {
    tonic::include_proto!("messages");
}