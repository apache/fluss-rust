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

use crate::metadata::TableBucket;
use crate::proto;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetDvForUnionReadRequest {
    pub inner_request: proto::GetDvForUnionReadRequest,
}

impl GetDvForUnionReadRequest {
    pub fn new(
        table_bucket: &TableBucket,
        requested_snapshot_id: i64,
        data_files: Vec<String>,
    ) -> Self {
        Self {
            inner_request: proto::GetDvForUnionReadRequest {
                table_id: table_bucket.table_id(),
                partition_id: table_bucket.partition_id(),
                bucket_id: table_bucket.bucket_id(),
                requested_snapshot_id,
                data_files,
            },
        }
    }
}

impl RequestBody for GetDvForUnionReadRequest {
    type ResponseBody = proto::GetDvForUnionReadResponse;
    const API_KEY: ApiKey = ApiKey::GetDvForUnionRead;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(GetDvForUnionReadRequest);
impl_read_version_type!(proto::GetDvForUnionReadResponse);
