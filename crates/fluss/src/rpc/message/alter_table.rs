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

use crate::rpc::api_key::ApiKey;
use crate::rpc::convert::to_table_path;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use crate::metadata::TablePath;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug, Default)]
pub struct AlterTableRequest {
    pub inner_request: proto::AlterTableRequest,
}

impl AlterTableRequest {
    pub fn new(
        table_path: &TablePath,
        ignore_if_not_exists: bool,
        config_changes: Vec<proto::PbAlterConfig>,
        add_columns: Vec<proto::PbAddColumn>,
        drop_columns: Vec<proto::PbDropColumn>,
        rename_columns: Vec<proto::PbRenameColumn>,
        modify_columns: Vec<proto::PbModifyColumn>,
    ) -> Self {
        AlterTableRequest {
            inner_request: proto::AlterTableRequest {
                table_path: to_table_path(table_path),
                ignore_if_not_exists,
                config_changes,
                add_columns,
                drop_columns,
                rename_columns,
                modify_columns,
            },
        }
    }
}

impl RequestBody for AlterTableRequest {
    type ResponseBody = proto::AlterTableResponse;
    const API_KEY: ApiKey = ApiKey::AlterTable;
}

impl_write_type!(AlterTableRequest);
impl_read_type!(proto::AlterTableResponse);
