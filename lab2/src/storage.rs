use kv::{storage_rpc::storage_server::StorageServer, storage::KvStorage};
use tonic::transport::Server;

use tracing_subscriber::{EnvFilter};

#[tokio::main]

async fn main() {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let storage = KvStorage::new();
    let addr = "127.0.0.1:50051";
    let storage = StorageServer::new(storage);
    let _= Server::builder()
        .add_service(storage)
        .serve(addr.parse().unwrap())
        .await
        .unwrap();
}