// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Error;
use crate::error::Result as FlussResult;
use crate::proto::ListOffsetsResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use std::collections::HashMap;

use bytes::{Buf, BufMut};
use prost::Message;

/// Offset type constants as per proto comments
pub const LIST_EARLIEST_OFFSET: i32 = 0;
pub const LIST_LATEST_OFFSET: i32 = 1;
pub const LIST_OFFSET_FROM_TIMESTAMP: i32 = 2;

/// Client follower server id constant
pub const CLIENT_FOLLOWER_SERVER_ID: i32 = -1;

/// Offset specification for list offsets request
#[derive(Debug, Clone)]
pub enum OffsetSpec {
    /// Earliest offset spec
    Earliest,
    /// Latest offset spec  
    Latest,
    /// Timestamp offset spec
    Timestamp(i64),
}

impl OffsetSpec {
    pub fn offset_type(&self) -> i32 {
        match self {
            OffsetSpec::Earliest => LIST_EARLIEST_OFFSET,
            OffsetSpec::Latest => LIST_LATEST_OFFSET,
            OffsetSpec::Timestamp(_) => LIST_OFFSET_FROM_TIMESTAMP,
        }
    }

    pub fn start_timestamp(&self) -> Option<i64> {
        match self {
            OffsetSpec::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    pub inner_request: proto::ListOffsetsRequest,
}

impl ListOffsetsRequest {
    pub fn new(
        table_id: i64,
        partition_id: Option<i64>,
        bucket_ids: Vec<i32>,
        offset_spec: OffsetSpec,
    ) -> Self {
        ListOffsetsRequest {
            inner_request: proto::ListOffsetsRequest {
                follower_server_id: CLIENT_FOLLOWER_SERVER_ID,
                offset_type: offset_spec.offset_type(),
                table_id,
                partition_id,
                bucket_id: bucket_ids,
                start_timestamp: offset_spec.start_timestamp(),
            },
        }
    }
}

impl RequestBody for ListOffsetsRequest {
    type ResponseBody = ListOffsetsResponse;

    const API_KEY: ApiKey = ApiKey::ListOffsets;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListOffsetsRequest);
impl_read_version_type!(ListOffsetsResponse);

impl ListOffsetsResponse {
    pub fn offsets(&self) -> FlussResult<HashMap<i32, i64>> {
        self.buckets_resp
            .iter()
            .map(|resp| {
                if resp.error_code.is_some() {
                    // todo: consider use another suitable error
                    Err(Error::WriteError(format!(
                        "Missing offset, error message: {}",
                        resp.error_message
                            .as_deref()
                            .unwrap_or("unknown server exception")
                    )))
                } else {
                    // if no error msg, offset must exists
                    Ok((resp.bucket_id, resp.offset.unwrap()))
                }
            })
            .collect()
    }
}
