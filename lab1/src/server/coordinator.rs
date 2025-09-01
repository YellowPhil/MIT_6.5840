pub struct Coordinator<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Sized + Send + Sync,
    Value: Sized + Send + Sync,
{
    workers: Vec<u32>,
    channel: tonic::transport::Channel,
    map: MapFn<Key, Value>,
    reduce: ReduceFn<Key, Value>,
}

pub type MapFn<Key, Value> = Box<dyn Fn(Key, Value) -> Vec<(Key, Value)>>;
pub type ReduceFn<Key, Value> = Box<dyn Fn(Vec<(Key, Value)>) -> Value>;

impl<Key, Value> Coordinator<Key, Value>
where
    Key: std::hash::Hash + std::cmp::Eq + Sized + Send + Sync,
    Value: Sized + Send + Sync,
{
    pub fn new(
        workers: Vec<u32>,
        channel: tonic::transport::Channel,
        map: MapFn<Key, Value>,
        reduce: ReduceFn<Key, Value>,
    ) -> Self {
        Self {
            workers,
            channel,
            map,
            reduce,
        }
    }
}
