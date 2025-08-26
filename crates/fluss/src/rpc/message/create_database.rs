use crate::metadata::DatabaseDescriptor;
use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::CreateDatabaseResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};

use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct CreateDatabaseRequest {
    pub inner_request: proto::CreateDatabaseRequest,
}

impl CreateDatabaseRequest {
    pub fn new(
        database_name: &str,
        ignore_if_exists: bool,
        database_descriptor: Option<&DatabaseDescriptor>,
    ) -> FlussResult<Self> {
        let database_json = if let Some(descriptor) = database_descriptor {
            Some(descriptor.to_json_bytes()?)
        } else {
            None
        };

        Ok(CreateDatabaseRequest {
            inner_request: proto::CreateDatabaseRequest {
                database_name: database_name.to_string(),
                ignore_if_exists,
                database_json,
            },
        })
    }
}

impl RequestBody for CreateDatabaseRequest {
    type ResponseBody = CreateDatabaseResponse;

    const API_KEY: ApiKey = ApiKey::CreateDatabase;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(CreateDatabaseRequest);
impl_read_version_type!(CreateDatabaseResponse);
