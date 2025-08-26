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

use crate::proto::{MetadataResponse, PbTablePath};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};

use crate::metadata::TablePath;
use crate::{impl_read_version_type, impl_write_version_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

pub struct UpdateMetadataRequest {
    pub inner_request: proto::MetadataRequest,
}

impl UpdateMetadataRequest {
    pub fn new(table_paths: &[&TablePath]) -> Self {
        UpdateMetadataRequest {
            inner_request: proto::MetadataRequest {
                table_path: table_paths
                    .iter()
                    .map(|path| PbTablePath {
                        database_name: path.database().to_string(),
                        table_name: path.table().to_string(),
                    })
                    .collect(),
                partitions_path: vec![],
                partitions_id: vec![],
            },
        }
    }
}

impl RequestBody for UpdateMetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::MetaData;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(UpdateMetadataRequest);
impl_read_version_type!(MetadataResponse);
