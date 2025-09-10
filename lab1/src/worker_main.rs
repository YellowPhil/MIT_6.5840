use std::io::{Cursor, Read};
use std::ops::{Deref, DerefMut};

use mapreduce::messages::map_worker_server::MapWorkerServer;
use mapreduce::messages::reduce_worker_server::ReduceWorkerServer;
use mapreduce::worker::{MapWorkerImpl, ReduceWorkerImpl};
use tokio::task::JoinSet;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status, Streaming};
use tracing_subscriber::EnvFilter;

use crate::fileinfo::{FileContents, FileName};

pub mod fileinfo {
    include!(concat!(env!("OUT_DIR"), "/fileinfo.rs"));
}

/// We'll try to do the grep test where we determine amount of lines containing a word
/// and split by space.
/// Map function will be:
/// - split by space
/// - emit key, value pairs
/// Reduce function will be:
/// - count the number of lines containing the word
///
/// Key will be the file name, value will be it's contents
/// 
impl Into<Box<dyn Read + 'static>> for FileName {
    fn into(self) -> Box<dyn Read + 'static> {
        Box::new(Cursor::new(self.name))
    }
}

/// Key will be the file name, value will be it's contents
fn map_fn(key: FileName, value: FileContents) -> Vec<(FileName, FileContents)> {
    let mut result = Vec::new();
    for line in value.contents.split('\n') {
        if line.contains("hello") {
            result.push((key.clone(), FileContents { contents: line.to_string() }));
        }
    }
    result
}

fn reduce_fn(_key: FileName, values: Vec<FileContents>) -> Vec<FileContents> {
    values
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let map_listen_addr = "127.0.0.1:50051";
    let reduce_listen_addr = "127.0.0.1:50052";
    let reduce_worker = ReduceWorkerServer::new(ReduceWorkerImpl::new(0, Box::new(reduce_fn)));
    let map_worker = MapWorkerServer::new(MapWorkerImpl::new(0, Box::new(map_fn), 1));

    let map_worker = Server::builder()
        .add_service(map_worker)
        .serve(map_listen_addr.parse().unwrap());

    let reduce_worker = Server::builder()
        .add_service(reduce_worker)
        .serve(reduce_listen_addr.parse().unwrap());

    let mut join_set = JoinSet::new();
    join_set.spawn(async move { map_worker.await.unwrap() });
    join_set.spawn(async move { reduce_worker.await.unwrap() });
    join_set.join_all().await;
}
