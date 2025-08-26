use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use crate::error::Result as FlussResult;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetDatabaseInfoRequest {
    pub inner_request: proto::GetDatabaseInfoRequest,
}

impl GetDatabaseInfoRequest {
    pub fn new(database_name: &str) -> FlussResult<Self> {
        Ok(GetDatabaseInfoRequest {
            inner_request: proto::GetDatabaseInfoRequest {
                database_name: database_name.to_string(),
            },
        })
    }
}

impl RequestBody for GetDatabaseInfoRequest {
    type ResponseBody = proto::GetDatabaseInfoResponse;

    const API_KEY: ApiKey = ApiKey::GetDatabaseInfo;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(GetDatabaseInfoRequest);
impl_read_version_type!(proto::GetDatabaseInfoResponse);
