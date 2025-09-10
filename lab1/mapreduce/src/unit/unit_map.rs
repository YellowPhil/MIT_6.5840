#[cfg(test)]
mod unit_tests_map_worker {
    use std::io::{Cursor, Read};
    use tokio_stream::{self as stream, Stream, StreamExt};
    use tonic::{IntoStreamingRequest, Request, Streaming};

    use crate::messages::{self, map_worker_server::MapWorkerServer};
    use crate::worker::MapWorkerImpl;

    #[derive(Clone, prost::Message)]
    struct CustomString {
        #[prost(string, tag = "1")]
        name: String,
    }
    impl From<String> for CustomString {
        fn from(value: String) -> Self {
            Self { name: value }
        }
    }
    impl Into<Box<dyn Read + 'static>> for CustomString {
        fn into(self) -> Box<dyn Read + 'static> {
            Box::new(Cursor::new(self.name))
        }
    }

    fn map_fn<T: Clone + Send + Sync + 'static>(key: T, value: T) -> Vec<(T, T)> {
        vec![(key, value)]
    }

    fn stream_of_requests() -> impl Stream<Item = messages::MapRequest> {
        tokio_stream::iter(vec![messages::MapRequest {
            values: vec![messages::Kv {
                key: "key".to_string().into(),
                value: "value".to_string().into(),
            }],
        }])
    }

}
