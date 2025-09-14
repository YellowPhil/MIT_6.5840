pub mod storage_rpc {
    tonic::include_proto!("storage");
}
pub mod google_rpc {
    tonic::include_proto!("google.rpc");
}

pub mod client;
pub mod storage;

