use crate::metadata::TablePath;
use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::DropTableResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::convert::to_table_path;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};

use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct DropTableRequest {
    pub inner_request: proto::DropTableRequest,
}

impl DropTableRequest {
    pub fn new(
        table_path: &TablePath,
        ignore_if_not_exists: bool,
    ) -> FlussResult<Self> {
        Ok(DropTableRequest {
            inner_request: proto::DropTableRequest {
                table_path: to_table_path(table_path),
                ignore_if_not_exists,
            },
        })
    }
}

impl RequestBody for DropTableRequest {
    type ResponseBody = DropTableResponse;

    const API_KEY: ApiKey = ApiKey::DropTable;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(DropTableRequest);
impl_read_version_type!(DropTableResponse);