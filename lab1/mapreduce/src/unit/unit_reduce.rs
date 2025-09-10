#[cfg(test)]
mod unit_tests_reduce_worker {
    use tonic::Request;

    use crate::{messages, worker::ReduceWorkerImpl};
    fn reduce_fn<T: Clone + Send + Sync + 'static>(_key: T, values: Vec<T>) -> Vec<T> {
        values
    }
    #[tokio::test]
    async fn reduce_worker_run_empty() { 
        use crate::messages::reduce_worker_server::ReduceWorker;
        let reduce_worker = ReduceWorkerImpl::new(0, Box::new(reduce_fn::<String>));
        let response = reduce_worker.run_reduce(Request::new(messages::ReduceRequest {
            partition_id: 0,
            map_output_locations: vec![],
        })).await.unwrap().into_inner();
        assert!(response.success);
    }
}