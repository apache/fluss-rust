use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use crate::error::Result as FlussResult;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct DatabaseExistsRequest {
    pub inner_request: proto::DatabaseExistsRequest,
}

impl DatabaseExistsRequest {
    pub fn new(database_name: &str) -> FlussResult<Self> {
        Ok(DatabaseExistsRequest {
            inner_request: proto::DatabaseExistsRequest {
                database_name: database_name.to_string(),
            },
        })
    }
}

impl RequestBody for DatabaseExistsRequest {
    type ResponseBody = proto::DatabaseExistsResponse;

    const API_KEY: ApiKey = ApiKey::DatabaseExists;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(DatabaseExistsRequest);
impl_read_version_type!(proto::DatabaseExistsResponse);
