mod coordinator;

use std::io::Cursor;

use coordinator::Coordinator;
use std::io::Read;
use tonic::{Request, Response, Status};

mod fileinfo {
    tonic::include_proto!("fileinfo");
}

impl Into<Box<dyn Read>> for fileinfo::FileName{
    fn into(self) -> Box<dyn Read> {
        Box::new(Cursor::new(self.name))
    }
}

impl From<&str> for fileinfo::FileName {
    fn from(value: &str) -> Self {
        fileinfo::FileName {
            name: value.to_string(),
        }
    }
}
impl From<String> for fileinfo::FileName {
    fn from(value: String) -> Self {
        fileinfo::FileName {
            name: value,
        }
    }
}

impl From<String> for fileinfo::FileContents{
    fn from(value: String) -> Self {
        fileinfo::FileContents {
            contents: value.into_bytes(),
        }
    }
}
impl From<&str> for fileinfo::FileContents{
    fn from(value: &str) -> Self {
        fileinfo::FileContents {
            contents: value.to_string().into_bytes(),
        }
    }
}

#[tokio::main]
async fn main() {
    let map_worker = std::env::args().nth(1).unwrap();
    let reduce_worker = std::env::args().nth(2).unwrap();
    let mut coordinator = Coordinator::new();

    coordinator.add_mapper(0, map_worker).await.unwrap();
    coordinator.add_reducer(0, reduce_worker).await.unwrap();

    coordinator
        .map(vec![
            (fileinfo::FileName::from("file1"), fileinfo::FileContents::from("contents1")),
            (fileinfo::FileName::from("file2"), fileinfo::FileContents::from("contents2")),
        ])
        .await
        .unwrap();
}
