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

use crate::metadata::{JsonSerde, TableDescriptor, TablePath};
use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::CreateTableResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::convert::to_table_path;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};

use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct CreateTableRequest {
    pub inner_request: proto::CreateTableRequest,
}

impl CreateTableRequest {
    pub fn new(
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> FlussResult<Self> {
        Ok(CreateTableRequest {
            inner_request: proto::CreateTableRequest {
                table_path: to_table_path(table_path),
                table_json: serde_json::to_vec(&table_descriptor.serialize_json()?).unwrap(),
                ignore_if_exists,
            },
        })
    }
}

impl RequestBody for CreateTableRequest {
    type ResponseBody = CreateTableResponse;

    const API_KEY: ApiKey = ApiKey::CreateTable;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(CreateTableRequest);
impl_read_version_type!(CreateTableResponse);
