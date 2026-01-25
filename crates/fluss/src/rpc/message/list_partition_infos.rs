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

use crate::metadata::{PartitionInfo, PartitionSpec, TablePath};
use crate::proto::ListPartitionInfosResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::convert::to_table_path;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct ListPartitionInfosRequest {
    pub inner_request: proto::ListPartitionInfosRequest,
}

impl ListPartitionInfosRequest {
    pub fn new(table_path: &TablePath, partial_partition_spec: Option<&PartitionSpec>) -> Self {
        ListPartitionInfosRequest {
            inner_request: proto::ListPartitionInfosRequest {
                table_path: to_table_path(table_path),
                partial_partition_spec: partial_partition_spec.map(|s| s.to_pb()),
            },
        }
    }
}

impl RequestBody for ListPartitionInfosRequest {
    type ResponseBody = ListPartitionInfosResponse;

    const API_KEY: ApiKey = ApiKey::ListPartitionInfos;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListPartitionInfosRequest);
impl_read_version_type!(ListPartitionInfosResponse);

impl ListPartitionInfosResponse {
    pub fn get_partitions_info(&self) -> Vec<PartitionInfo> {
        self.partitions_info
            .iter()
            .map(PartitionInfo::from_pb)
            .collect()
    }
}
