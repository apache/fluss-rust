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

use crate::metadata::TablePath;
use crate::proto;
use crate::proto::PbTablePath;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetLakeSnapshotRequest {
    pub inner_request: proto::GetLakeSnapshotRequest,
}

impl GetLakeSnapshotRequest {
    pub fn latest(table_path: &TablePath) -> Self {
        Self {
            inner_request: proto::GetLakeSnapshotRequest {
                table_path: PbTablePath {
                    database_name: table_path.database().to_string(),
                    table_name: table_path.table().to_string(),
                },
                snapshot_id: None,
                readable: None,
            },
        }
    }

    pub fn readable(table_path: &TablePath) -> Self {
        Self {
            inner_request: proto::GetLakeSnapshotRequest {
                table_path: PbTablePath {
                    database_name: table_path.database().to_string(),
                    table_name: table_path.table().to_string(),
                },
                snapshot_id: None,
                readable: Some(true),
            },
        }
    }

    pub fn by_id(table_path: &TablePath, snapshot_id: i64) -> Self {
        Self {
            inner_request: proto::GetLakeSnapshotRequest {
                table_path: PbTablePath {
                    database_name: table_path.database().to_string(),
                    table_name: table_path.table().to_string(),
                },
                snapshot_id: Some(snapshot_id),
                readable: None,
            },
        }
    }
}

impl RequestBody for GetLakeSnapshotRequest {
    type ResponseBody = proto::GetLakeSnapshotResponse;
    const API_KEY: ApiKey = ApiKey::GetLatestLakeSnapshot;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(GetLakeSnapshotRequest);
impl_read_version_type!(proto::GetLakeSnapshotResponse);
