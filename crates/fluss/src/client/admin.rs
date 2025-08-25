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

use crate::client::metadata::Metadata;
use crate::metadata::{JsonSerde, TableDescriptor, TableInfo, TablePath};
use crate::rpc::message::{CreateTableRequest, GetTableRequest};
use crate::rpc::{RpcClient, ServerConnection};
use std::sync::Arc;

use crate::error::Result;
use crate::proto::GetTableInfoResponse;

#[allow(dead_code)]
pub struct FlussAdmin {
    admin_gateway: ServerConnection,
    metadata: Arc<Metadata>,
    rpc_client: Arc<RpcClient>,
}

impl FlussAdmin {
    pub async fn new(connections: Arc<RpcClient>, metadata: Arc<Metadata>) -> Result<Self> {
        let admin_con = connections
            .get_connection(
                metadata
                    .get_cluster()
                    .get_coordinator_server()
                    .expect("Couldn't coordinator server"),
            )
            .await?;

        Ok(FlussAdmin {
            admin_gateway: admin_con,
            metadata,
            rpc_client: connections,
        })
    }

    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(CreateTableRequest::new(
                table_path,
                table_descriptor,
                ignore_if_exists,
            )?)
            .await?;
        Ok(())
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<TableInfo> {
        let response = self
            .admin_gateway
            .request(GetTableRequest::new(table_path))
            .await?;
        let GetTableInfoResponse {
            table_id,
            schema_id,
            table_json,
            created_time,
            modified_time,
        } = response;
        let v: &[u8] = &table_json[..];
        let table_descriptor =
            TableDescriptor::deserialize_json(&serde_json::from_slice(v).unwrap())?;
        Ok(TableInfo::of(
            table_path.clone(),
            table_id,
            schema_id,
            table_descriptor,
            created_time,
            modified_time,
        ))
    }
}
