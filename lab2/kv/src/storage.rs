use dashmap::DashMap;
use std::sync::Arc;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use crate::storage_rpc;

pub struct KvStorage {
    inner: Arc<DashMap<String, (String, u64)>>,
}
impl KvStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl storage_rpc::storage_server::Storage for KvStorage {
    async fn put(
        &self,
        request: Request<storage_rpc::PutRequest>,
    ) -> Result<Response<storage_rpc::PutResponse>, Status> {
        tracing::info!("Put request received");

        let request = request.into_inner();
        let key =
            String::from_utf8(request.key).map_err(|_| Status::invalid_argument("Invalid key"))?;

        let mut metadata = MetadataMap::new();
        if let Some(entry) = self.inner.get(&key) {
            let (value, version) = entry.value();
            tracing::debug!("PUT {key};  request version: {};  stored version: {}", request.version, version);
            if *version != request.version {
                tracing::warn!("PUT {key};  request version: {};  stored version: {}", request.version, version);
                metadata.insert("version", version.to_string().parse().unwrap());
                metadata.insert("data", value.parse().unwrap());
                return Err(Status::with_metadata(
                    tonic::Code::FailedPrecondition,
                    "Version mismatch",
                    metadata,
                ))
            }
        } else {
            if request.version != 0 {
                tracing::warn!("PUT {key};  request version: {};  required version: {}", request.version, 0);
                metadata.insert("version", "0".parse().unwrap());
                return Err(Status::with_metadata(
                    tonic::Code::FailedPrecondition,
                    "Version must be 0",
                    metadata,
                ))
            }
        }

        let value = String::from_utf8(request.value)
            .map_err(|_| Status::invalid_argument("Invalid value"))?;
        tracing::debug!("PUT {key};  request value: {}", value);

        let new_version = self.inner.get_mut(&key).map(|entry| entry.1+1).unwrap_or(1);
        tracing::debug!("PUT {key};  new version: {}", new_version);
        self.inner.insert(key, (value, new_version));

        tracing::info!("Put request completed");

        Ok(Response::new(storage_rpc::PutResponse {
            version: new_version,
        }))
    }

    async fn get(
        &self,
        request: Request<storage_rpc::GetRequest>,
    ) -> Result<Response<storage_rpc::GetResponse>, Status> {
        tracing::info!("Get request received");

        let request = request.into_inner();
        let key =
            String::from_utf8(request.key).map_err(|_| Status::invalid_argument("Invalid key"))?;

        let Some(entry) = self.inner.get(&key) else {
            tracing::warn!("GET {key};  key does not exist");
            return Err(Status::not_found("Key does not exist"));
        };
        let (value, version) = entry.value();
        tracing::debug!("GET {key};  request value: {}", value);
        tracing::debug!("GET {key};  request version: {}", version);

        tracing::info!("GET {key};  request successful");

        Ok(Response::new(storage_rpc::GetResponse {
            version: *version,
            value: value.as_bytes().to_vec(),
        }))
    }

    async fn delete(
        &self,
        request: Request<storage_rpc::DeleteRequest>,
    ) -> Result<Response<storage_rpc::DeleteResponse>, Status> {
        let request = request.into_inner();
        let key =
            String::from_utf8(request.key).map_err(|_| Status::invalid_argument("Invalid key"))?;

        let Some(entry) = self.inner.get(&key) else {
            return Err(Status::not_found("Key does not exist"));
        };
        let (_, version) = entry.value();
        if *version != request.version {
            return Err(Status::failed_precondition("Version mismatch"));
        }
        self.inner.remove(&key);
        Ok(Response::new(storage_rpc::DeleteResponse {}))
    }
}
