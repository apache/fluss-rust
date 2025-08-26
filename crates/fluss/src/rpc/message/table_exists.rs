use crate::metadata::TablePath;
use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::TableExistsResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::convert::to_table_path;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};

use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct TableExistsRequest {
    pub inner_request: proto::TableExistsRequest,
}

impl TableExistsRequest {
    pub fn new(table_path: &TablePath) -> FlussResult<Self> {
        Ok(TableExistsRequest {
            inner_request: proto::TableExistsRequest {
                table_path: to_table_path(table_path),
            },
        })
    }
}

impl RequestBody for TableExistsRequest {
    type ResponseBody = TableExistsResponse;

    const API_KEY: ApiKey = ApiKey::TableExists;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(TableExistsRequest);
impl_read_version_type!(TableExistsResponse);
