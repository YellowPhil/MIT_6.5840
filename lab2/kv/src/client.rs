use dashmap::DashMap;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::{Request, Status, transport::Channel};

use crate::{storage_rpc, storage_rpc::storage_client::StorageClient};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    ConnectionError(tonic::transport::Error),
    #[error("Timeout error: {0}")]
    TimeoutError(Status),
    #[error("Request error: {0}")]
    RequestError(Status),
    #[error("Invalid key: {0}")]
    InvalidKey(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

pub struct Client {
    connect_addr: String,
    connection: RwLock<Option<StorageClient<Channel>>>,
    timeout: Duration,
    retry_count: usize,
    keys_version: DashMap<String, u64>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            connect_addr: Default::default(),
            connection: RwLock::new(None),
            timeout: Duration::from_secs(10),
            retry_count: 3,
            keys_version: DashMap::new(),
        }
    }
}

impl Client {
    pub fn new(addr: &str) -> Self {
        Self {
            connect_addr: addr.to_string(),
            ..Default::default()
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    pub fn set_retry_count(&mut self, retry_count: usize) {
        self.retry_count = retry_count;
    }

    async fn get_connection(&self) -> Result<StorageClient<Channel>, ClientError> {
        if let Some(connection) = &*self.connection.read().await {
            return Ok(connection.clone());
        }

        let connection = StorageClient::connect(self.connect_addr.clone())
            .await
            .map_err(|e| ClientError::ConnectionError(e))?;

        *self.connection.write().await = Some(connection.clone());
        Ok(connection)
    }

    pub async fn with_connection(
        &mut self,
        with_fn: impl FnOnce(StorageClient<Channel>),
    ) -> Result<(), ClientError> {
        let connection = self.get_connection().await?;
        with_fn(connection);
        Ok(())
    }

    pub async fn put(&mut self, key: &str, value: &str) -> Result<u64, ClientError> {
        // let mut connection = self.get_connection().await?;
        let mut connection = StorageClient::connect(self.connect_addr.clone())
            .await
            .map_err(|e| ClientError::ConnectionError(e))?;

        let current_version = {
            *self
                .keys_version
                .entry(key.to_string())
                .or_insert(0)
                .value()
        };

        let mut request = storage_rpc::PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            version: current_version,
        };

        for _ in 0..self.retry_count {
            let mut prepared_request = Request::new(request.clone());
            prepared_request.set_timeout(self.timeout);
            let result = connection.put(prepared_request).await;

            match result {
                Ok(response) => {
                    let version = response.into_inner().version;
                    self.keys_version.insert(key.to_string(), version);
                    return Ok(version);
                }
                Err(status) => match status.code() {
                    tonic::Code::FailedPrecondition => {
                        // TODO: bytes to u64 is 2.5 times faster
                        let version = status
                            .metadata()
                            .get("version")
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap();
                        self.keys_version.insert(key.to_string(), version);
                        request.version = version;
                        continue;
                    }
                    _ => {
                        return Err(ClientError::RequestError(status));
                    }
                },
            }
        }

        Err(ClientError::RequestError(Status::internal(
            "Failed to put key",
        )))
    }

    pub async fn get(&mut self, key: &str) -> Result<String, ClientError> {
        // let mut connection = self.get_connection().await?;
        let mut connection = StorageClient::connect(self.connect_addr.clone())
            .await
            .map_err(|e| ClientError::ConnectionError(e))?;
        let request = storage_rpc::GetRequest {
            key: key.as_bytes().to_vec(),
        };

        for _ in 0..self.retry_count {
            let mut prepared_request = Request::new(request.clone());
            prepared_request.set_timeout(self.timeout);

            match connection.get(prepared_request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    self.keys_version.insert(key.to_string(), response.version);
                    return Ok(String::from_utf8(response.value).unwrap());
                }
                Err(status) => match status.code() {
                    tonic::Code::NotFound | tonic::Code::InvalidArgument => {
                        return Err(ClientError::RequestError(status));
                    }
                    _ => {
                        continue;
                    }
                },
            }
        }
        Err(ClientError::RequestError(Status::internal(
            "Failed to get key",
        )))
    }
}

#[cfg(test)]
mod test_client {

    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use tokio::task::{JoinHandle, JoinSet};
    use tonic::transport::Server;

    use super::*;

    static ADDRESS: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

    async fn spawn_server() -> String {
        let addr = "127.0.0.1:50054";
        let server = Server::builder().add_service(
            storage_rpc::storage_server::StorageServer::new(crate::storage::KvStorage::new()),
        );
        tokio::spawn(async move {
            let _ = server.serve(addr.parse().unwrap()).await.unwrap();
        });
        tokio::time::sleep(Duration::from_secs(1)).await;

        format!("http://{addr}")
    }
    fn random_string(length: usize) -> String {
        let mut rng = rand::rng();
        let s: String = std::iter::repeat(())
            .map(|()| rng.sample(rand::distr::Alphanumeric))
            .map(char::from)
            .take(length)
            .collect();
        s
    }
    #[tokio::test]
    async fn test_invalid_get() {
        let addr = ADDRESS.get_or_init(spawn_server).await;
        let mut client = Client::new(addr);
        let key = random_string(7);
        let version = client.get(&key).await;
        assert!(version.is_err());
    }

    #[tokio::test]
    async fn test_put_get_cycle() {
        let addr = ADDRESS.get_or_init(spawn_server).await;
        let mut client = Client::new(addr);
        let key = random_string(7);
        let value = random_string(7);
        let value2 = random_string(7);

        let version = client.put(&key, &value).await.unwrap();
        println!("Version: {}", version);
        assert_eq!(version, 1);

        let value = client.get(&key).await.unwrap();
        println!("Value: {}", value);
        assert_eq!(value, value);

        let version = client.put(&key, &value2).await.unwrap();
        println!("Version: {}", version);
        assert_eq!(version, 2);

        let value = client.get(&key).await.unwrap();
        println!("Value: {}", value);
        assert_eq!(value, value2);
    }

    #[tokio::test]
    async fn test_concurrent_put() {
        let addr = ADDRESS.get_or_init(spawn_server).await;
        let mut client1 = Client::new(addr);
        let mut client2 = Client::new(addr);
        let key = random_string(7);
        let value = random_string(7);
        let value2 = random_string(7);

        let version = client1.put(&key, &value).await.unwrap();
        println!("Version: {}", version);
        assert_eq!(version, 1);
        let version = client2.put(&key, &value2).await.unwrap();
        println!("Version: {}", version);
        assert_eq!(version, 2);
    }
}
