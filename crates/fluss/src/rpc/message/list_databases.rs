use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use crate::error::Result as FlussResult;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct ListDatabasesRequest {
    pub inner_request: proto::ListDatabasesRequest,
}

impl ListDatabasesRequest {
    pub fn new() -> FlussResult<Self> {
        Ok(ListDatabasesRequest {
            inner_request: proto::ListDatabasesRequest {}
        })
    }
}

impl RequestBody for ListDatabasesRequest {
    type ResponseBody = proto::ListDatabasesResponse;

    const API_KEY: ApiKey = ApiKey::ListDatabases;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListDatabasesRequest);
impl_read_version_type!(proto::ListDatabasesResponse);
