pub mod coordinator;
pub mod worker;
mod unit;

pub mod messages {
    tonic::include_proto!("messages");
}