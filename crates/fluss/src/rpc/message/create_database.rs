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
