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

use crate::error::Result as FlussResult;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
use crate::{impl_read_version_type, impl_write_version_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct DropDatabaseRequest {
    pub inner_request: proto::DropDatabaseRequest,
}

impl DropDatabaseRequest {
    pub fn new(
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> FlussResult<Self> {
        let mut inner_request = proto::DropDatabaseRequest::default();
        inner_request.database_name = database_name.to_string();
        inner_request.ignore_if_not_exists = ignore_if_not_exists;
        inner_request.cascade = cascade;

        Ok(Self { inner_request })
    }
}

impl RequestBody for DropDatabaseRequest {
    type ResponseBody = proto::DropDatabaseResponse;

    const API_KEY: ApiKey = ApiKey::DropDatabase;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(DropDatabaseRequest);
impl_read_version_type!(proto::DropDatabaseResponse);
