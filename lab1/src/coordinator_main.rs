use std::io::{Cursor, Read};
use prost::Message;

use fileinfo::{FileContents, FileName};
use mapreduce::{
    coordinator::coordinator::Coordinator,
    messages::{self, reduce_worker_client::ReduceWorkerClient},
};
use tonic::Request;
use tracing_subscriber::EnvFilter;

pub mod fileinfo {
    include!(concat!(env!("OUT_DIR"), "/fileinfo.rs"));
}

impl Into<Box<dyn Read + 'static>> for FileName {
    fn into(self) -> Box<dyn Read + 'static> {
        Box::new(Cursor::new(self.name))
    }
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
    let mut coordinator = Coordinator::new();

    coordinator
        .add_reducer(0, "http://127.0.0.1:50052".parse().unwrap())
        .await
        .unwrap();
    coordinator
        .add_mapper(0, "http://127.0.0.1:50051".parse().unwrap())
        .await
        .unwrap();

    coordinator
        .map(vec![(
            FileName {
                name: "test.txt".to_string(),
            },
            FileContents {
                contents: "hello world!!".to_string(),
            },
        )])
        .await
        .unwrap();

    let fetch_request = Request::new(messages::Empty {});
    let mut reducer = ReduceWorkerClient::connect("http://127.0.0.1:50052")
        .await
        .unwrap();
    let response = reducer.fetch(fetch_request).await.unwrap().into_inner();
    for value in response.values {
        let key = FileName::decode(value.key.as_slice()).unwrap();
        let value = FileContents::decode(value.value.as_slice()).unwrap();
        println!("Key: {:?}, Value: {:?}", key, value);
    }
}
