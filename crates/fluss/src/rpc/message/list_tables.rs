use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::ListTablesResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};

use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct ListTablesRequest {
    pub inner_request: proto::ListTablesRequest,
}

impl ListTablesRequest {
    pub fn new(database_name: &str) -> FlussResult<Self> {
        Ok(ListTablesRequest {
            inner_request: proto::ListTablesRequest {
                database_name: database_name.to_string(),
            },
        })
    }
}

impl RequestBody for ListTablesRequest {
    type ResponseBody = ListTablesResponse;

    const API_KEY: ApiKey = ApiKey::ListTables;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListTablesRequest);
impl_read_version_type!(ListTablesResponse);
