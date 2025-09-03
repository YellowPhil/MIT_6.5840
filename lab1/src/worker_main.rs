use std::io::{Cursor, Read};
use std::ops::{Deref, DerefMut};

use mapreduce::messages::map_worker_server::MapWorkerServer;
use mapreduce::messages::reduce_worker_server::ReduceWorkerServer;
use mapreduce::worker::{MapWorker, ReduceWorker};
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status, Streaming};

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
impl Into<Box<dyn Read + Send + Sync>> for FileName {
    fn into(self) -> Box<dyn Read + Send + Sync> {
        Box::new(Cursor::new(self.name))
    }
}

pub(crate) struct FileReducerWorker(ReduceWorker<FileName, FileContents>);

impl FileReducerWorker {
    pub fn new(id: u32, reduce_fn: Box<dyn Fn(FileName, Vec<FileContents>) -> Vec<FileContents> + Send + Sync>) -> Self {
        Self(ReduceWorker::new(id, reduce_fn))
    }
}

impl Deref for FileReducerWorker {
    type Target = ReduceWorker<FileName, FileContents>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FileReducerWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
struct FileMapWorker(MapWorker<FileName, FileContents>);

impl FileMapWorker {
    pub fn new(id: u32, map_fn: Box<dyn Fn(FileName, FileContents) -> Vec<(FileName, FileContents)> + Send + Sync>) -> Self {
        Self(MapWorker::new(id, map_fn))
    }
}

impl Deref for FileMapWorker {
    type Target = MapWorker<FileName, FileContents>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for FileMapWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


#[async_trait]
impl mapreduce::messages::reduce_worker_server::ReduceWorker for FileReducerWorker {
    async fn run_reduce(
        &self,
        request: Request<mapreduce::messages::ReduceRequest>,
    ) -> Result<Response<mapreduce::messages::BasicResponse>, Status> {
        self.run_reduce(request).await
    }
    async fn halt(
        &self,
        request: Request<mapreduce::messages::Empty>,
    ) -> Result<Response<mapreduce::messages::BasicResponse>, Status> {
        self.halt(request).await
    }
}

#[async_trait]
impl mapreduce::messages::map_worker_server::MapWorker for FileMapWorker {
    async fn run_map(
        &self,
        request: Request<Streaming<mapreduce::messages::MapRequest>>,
    ) -> Result<Response<mapreduce::messages::BasicResponse>, Status> {
        self.run_map(request).await
    }
    async fn notify_new_reducer(
        &self,
        request: Request<mapreduce::messages::Empty>,
    ) -> Result<Response<mapreduce::messages::Empty>, Status> {
        self.notify_new_reducer(request).await
    }
    async fn fetch_partition(
        &self,
        request: Request<mapreduce::messages::FetchPartitionRequest>,
    ) -> Result<Response<mapreduce::messages::FetchPartitionResponse>, Status> {
        self.fetch_partition(request).await
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
    let listen_addr = "127.0.0.1:50051";
    let reduce_worker = ReduceWorkerServer::new(FileReducerWorker::new(0, Box::new(reduce_fn)));
    let map_worker = MapWorkerServer::new(FileMapWorker::new(0, Box::new(map_fn)));
    let _server = Server::builder()
        .add_service(reduce_worker)
        .add_service(map_worker)
        .serve(listen_addr.parse().unwrap())
        .await
        .unwrap();
}
