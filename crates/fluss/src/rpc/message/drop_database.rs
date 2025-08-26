use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use crate::error::Result as FlussResult;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct DropDatabaseRequest {
    pub inner_request: proto::DropDatabaseRequest,
}

impl DropDatabaseRequest {
    pub fn new(
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> FlussResult<Self> {
        let mut inner_request = proto::DropDatabaseRequest::default();
        inner_request.database_name = database_name.to_string();
        inner_request.ignore_if_not_exists = ignore_if_not_exists;
        inner_request.cascade = cascade;

        Ok(Self { inner_request })
    }
}

impl RequestBody for DropDatabaseRequest {
    type ResponseBody = proto::DropDatabaseResponse;

    const API_KEY: ApiKey = ApiKey::DropDatabase;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(DropDatabaseRequest);
impl_read_version_type!(proto::DropDatabaseResponse);
