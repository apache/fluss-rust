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
use crate::metadata::{DatabaseDescriptor, DatabaseInfo, JsonSerde, PhysicalTablePath, TableBucket, TableDescriptor, TableInfo, TablePath, LakeSnapshot};
use crate::rpc::{RpcClient, ServerConnection};
use crate::rpc::message::{GetTableRequest, CreateTableRequest, DropTableRequest, CreateDatabaseRequest, ListTablesRequest, TableExistsRequest, DropDatabaseRequest, ListDatabasesRequest, DatabaseExistsRequest, GetDatabaseInfoRequest, OffsetSpec, ListOffsetsRequest, ListOffsetsResult, GetLatestLakeSnapshotRequest};

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::proto::GetTableInfoResponse;

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

    pub async fn create_database(
        &self,
        database_name: &str,
        ignore_if_exists: bool,
        database_descriptor: Option<&DatabaseDescriptor>,
    ) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(CreateDatabaseRequest::new(
                database_name,
                ignore_if_exists,
                database_descriptor,
            )?)
            .await?;
        Ok(())
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

    pub async fn drop_table(
        &self,
        table_path: &TablePath,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(DropTableRequest::new(
                table_path,
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

    /// List all tables in the given database
    pub async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        let response = self
            .admin_gateway
            .request(ListTablesRequest::new(database_name)?)
            .await?;
        Ok(response.table_name)
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table_path: &TablePath) -> Result<bool> {
        let response = self
            .admin_gateway
            .request(TableExistsRequest::new(table_path)?)
            .await?;
        Ok(response.exists)
    }

    /// Drop a database
    pub async fn drop_database(
        &self,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(DropDatabaseRequest::new(database_name, ignore_if_not_exists, cascade)?)
            .await?;
        Ok(())
    }

    /// List all databases
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let response = self
            .admin_gateway
            .request(ListDatabasesRequest::new()?)
            .await?;
        Ok(response.database_name)
    }

    /// Check if a database exists
    pub async fn database_exists(&self, database_name: &str) -> Result<bool> {
        let response = self
            .admin_gateway
            .request(DatabaseExistsRequest::new(database_name)?)
            .await?;
        Ok(response.exists)
    }

    /// Get database information
    pub async fn get_database_info(&self, database_name: &str) -> Result<DatabaseInfo> {
        let request = GetDatabaseInfoRequest::new(database_name)?;
        let response = self
            .admin_gateway
            .request(request)
            .await?;
        
        // Convert proto response to DatabaseInfo
        let database_descriptor = DatabaseDescriptor::from_json_bytes(&response.database_json)?;
        
        Ok(DatabaseInfo::new(
            database_name.to_string(),
            database_descriptor,
            response.created_time,
            response.modified_time,
        ))
    }

    pub async fn list_offsets(
        &self,
        table_path: PhysicalTablePath,
        // partition_id: Option<i64>,
        buckets: &[i32],
        offset_spec: OffsetSpec,
    ) -> Result<ListOffsetsResult> {
        // TODO: support partition_id
        let partition_id = None;

        self.metadata.check_and_update_table_metadata(&[table_path.get_table_path().clone()]).await?;

        let cluster = self.metadata.get_cluster();
        let table_id_map = cluster.get_table_id_by_path();
        let table_id = table_id_map
            .get(table_path.get_table_path())
            .copied()
            .ok_or_else(|| crate::error::Error::InvalidTableError(format!(
                "Table not found: {}", table_path.get_table_path()
            )))?;
        
        // Prepare requests
        let request_map = self.prepare_list_offsets_requests(
            table_id,
            partition_id,
            buckets,
            offset_spec
        )?;

        // Create channels for results
        let mut bucket_to_offset_map = HashMap::new();
        let mut senders = HashMap::new();
        
        for &bucket in buckets {
            let (sender, receiver) = oneshot::channel();
            bucket_to_offset_map.insert(bucket, receiver);
            senders.insert(bucket, sender);
        }

        // Send Requests
        self.send_list_offsets_request(request_map, senders).await?;

        Ok(ListOffsetsResult::new(bucket_to_offset_map))
    }

    fn prepare_list_offsets_requests(
        &self, 
        table_id: i64,
        partition_id: Option<i64>,
        buckets: &[i32],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, ListOffsetsRequest>> {
        let cluster = self.metadata.get_cluster();
        let mut node_for_bucket_list: HashMap<i32, Vec<i32>> = HashMap::new();
        
        for &bucket_id in buckets {
            let table_bucket = TableBucket::new(table_id, bucket_id);
            let leader = cluster.leader_for(&table_bucket)
                .ok_or_else(|| crate::error::Error::InvalidTableError(
                    format!("No leader found for table bucket: table_id={}, bucket_id={}", table_id, bucket_id)
                ))?;
            
            node_for_bucket_list
                .entry(leader.id())
                .or_insert_with(Vec::new)
                .push(bucket_id);
        }

        let mut list_offsets_requests = HashMap::new();
        for(leader_id, bucket_ids) in node_for_bucket_list {
            let request = Self::make_list_offsets_request(
                table_id,
                partition_id,
                bucket_ids,
                offset_spec.clone(),
            )?;
            list_offsets_requests.insert(leader_id, request);
        }

        Ok(list_offsets_requests)
    }

    fn make_list_offsets_request(
        table_id: i64,
        partition_id: Option<i64>,
        bucket_ids: Vec<i32>,
        offset_spec: OffsetSpec,
    ) -> Result<ListOffsetsRequest> {
        ListOffsetsRequest::new(table_id, partition_id, bucket_ids, offset_spec)
    }

    async fn send_list_offsets_request(
        &self,
        request_map: HashMap<i32, ListOffsetsRequest>,
        mut senders: HashMap<i32, oneshot::Sender<Result<i64>>>,
    ) -> Result<()> {
        let mut tasks = Vec::new();
        
        for (leader_id, request) in request_map {
            let rpc_client = self.rpc_client.clone();
            let metadata = self.metadata.clone();
            
            let mut bucket_senders = HashMap::new();
            for &bucket_id in &request.inner_request.bucket_id {
                if let Some(sender) = senders.remove(&bucket_id) {
                    bucket_senders.insert(bucket_id, sender);
                }
            }
            
            let task = tokio::spawn(async move {
                let cluster = metadata.get_cluster();
                let tablet_server = cluster.get_tablet_server(leader_id)
                    .ok_or_else(|| crate::error::Error::InvalidTableError(
                        format!("Tablet server {} not found", leader_id)
                    ))?;
                
                let connection = rpc_client.get_connection(tablet_server).await?;
            
                let response = connection.request(request).await?;
                
                for bucket_resp in response.buckets_resp {
                    if let Some(sender) = bucket_senders.remove(&bucket_resp.bucket_id) {
                        let result = if let Some(error_code) = bucket_resp.error_code {
                            if error_code != 0 {
                                Err(crate::error::Error::WriteError(
                                    bucket_resp.error_message
                                        .unwrap_or_else(|| format!("Error code: {}", error_code))
                                ))
                            } else {
                                Ok(bucket_resp.offset.unwrap_or(0))
                            }
                        } else {
                            Ok(bucket_resp.offset.unwrap_or(0))
                        };
                        
                        let _ = sender.send(result);
                    }
                }
                
                Ok::<(), crate::error::Error>(())
            });
            
            tasks.push(task);
        }
        
        for task in tasks {
            task.await
                .map_err(|e| crate::error::Error::WriteError(format!("Task join error: {}", e)))??;
        }
        
        Ok(())
    }

    /// Get the latest lake snapshot for a table
    pub async fn get_latest_lake_snapshot(&self, table_path: &TablePath) -> Result<LakeSnapshot> {
        let response = self
            .admin_gateway
            .request(GetLatestLakeSnapshotRequest::new(table_path))
            .await?;

        // Convert proto response to LakeSnapshot
        let mut table_buckets_offset = HashMap::new();
        for bucket_snapshot in response.bucket_snapshots {
            let table_bucket = TableBucket::new(response.table_id, bucket_snapshot.bucket_id);
            if let Some(log_offset) = bucket_snapshot.log_offset {
                table_buckets_offset.insert(table_bucket, log_offset);
            }
        }

        Ok(LakeSnapshot::new(response.snapshot_id, table_buckets_offset))
    }
}
