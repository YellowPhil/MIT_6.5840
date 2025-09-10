use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

pub mod map_worker;
pub mod reduce_worker;

pub struct MapWorkerImpl<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    id: u32,
    local_cache: DashMap<BucketId, Vec<(Key, Value)>>,
    map_fn: MapFn<Key, Value>,
    reducers_amount: Mutex<usize>,
}

impl<Key, Value> MapWorkerImpl<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    pub fn new(id: u32, map_fn: MapFn<Key, Value>, reducers_amount: usize) -> Self {
        Self {
            id,
            local_cache: DashMap::new(),
            map_fn,
            reducers_amount: Mutex::new(reducers_amount),
        }
    }
}

pub struct ReduceWorkerImpl<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    id: u32,
    // TODO: this design choice may be suboptimal, need some storage abstraction
    local_cache: Arc<RwLock<Vec<(Key, Value)>>>,
    reduce_fn: ReduceFn<Key, Value>,
}

pub type MapFn<Key, Value> = Box<dyn Fn(Key, Value) -> Vec<(Key, Value)> + Send + Sync>;
pub type ReduceFn<Key, Value> = Box<dyn Fn(Key, Vec<Value>) -> Vec<Value> + Send + Sync>;
pub type BucketId = u32;

impl<Key, Value> ReduceWorkerImpl<Key, Value>
where
    Key: Send + Sync,
    Value: Send + Sync,
{
    pub fn new(id: u32, reduce_fn: ReduceFn<Key, Value>) -> Self {
        Self {
            id,
            local_cache: Arc::new(RwLock::new(Vec::new())),
            reduce_fn,
        }
    }
}
