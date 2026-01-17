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
use crate::metadata::{
    DatabaseDescriptor, DatabaseInfo, JsonSerde, LakeSnapshot, TableBucket, TableDescriptor,
    TableInfo, TablePath,
};
use crate::rpc::message::{
    CreateDatabaseRequest, CreateTableRequest, DatabaseExistsRequest, DropDatabaseRequest,
    DropTableRequest, GetDatabaseInfoRequest, GetLatestLakeSnapshotRequest, GetTableRequest,
    ListDatabasesRequest, ListTablesRequest, TableExistsRequest,
};
use crate::rpc::message::{ListOffsetsRequest, OffsetSpec};
use crate::rpc::{RpcClient, ServerConnection};

use crate::BucketId;
use crate::error::{Error, Result};
use crate::proto::GetTableInfoResponse;
use std::collections::HashMap;
use std::slice::from_ref;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct FlussAdmin {
    admin_gateway: ServerConnection,
    #[allow(dead_code)]
    metadata: Arc<Metadata>,
    #[allow(dead_code)]
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

    pub async fn drop_table(&self, table_path: &TablePath, ignore_if_exists: bool) -> Result<()> {
        let _response = self
            .admin_gateway
            .request(DropTableRequest::new(table_path, ignore_if_exists))
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
            .request(ListTablesRequest::new(database_name))
            .await?;
        Ok(response.table_name)
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table_path: &TablePath) -> Result<bool> {
        let response = self
            .admin_gateway
            .request(TableExistsRequest::new(table_path))
            .await?;
        Ok(response.exists)
    }

    /// Drop a database
    pub async fn drop_database(
        &self,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) {
        let _response = self
            .admin_gateway
            .request(DropDatabaseRequest::new(
                database_name,
                ignore_if_not_exists,
                cascade,
            ))
            .await;
    }

    /// List all databases
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let response = self
            .admin_gateway
            .request(ListDatabasesRequest::new())
            .await?;
        Ok(response.database_name)
    }

    /// Check if a database exists
    pub async fn database_exists(&self, database_name: &str) -> Result<bool> {
        let response = self
            .admin_gateway
            .request(DatabaseExistsRequest::new(database_name))
            .await?;
        Ok(response.exists)
    }

    /// Get database information
    pub async fn get_database_info(&self, database_name: &str) -> Result<DatabaseInfo> {
        let request = GetDatabaseInfoRequest::new(database_name);
        let response = self.admin_gateway.request(request).await?;

        // Convert proto response to DatabaseInfo
        let database_descriptor = DatabaseDescriptor::from_json_bytes(&response.database_json)?;

        Ok(DatabaseInfo::new(
            database_name.to_string(),
            database_descriptor,
            response.created_time,
            response.modified_time,
        ))
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

        Ok(LakeSnapshot::new(
            response.snapshot_id,
            table_buckets_offset,
        ))
    }

    /// List offset for the specified buckets. This operation enables to find the beginning offset,
    /// end offset as well as the offset matching a timestamp in buckets.
    pub async fn list_offsets(
        &self,
        table_path: &TablePath,
        buckets_id: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, i64>> {
        self.metadata
            .check_and_update_table_metadata(from_ref(table_path))
            .await?;

        if buckets_id.is_empty() {
            return Err(Error::UnexpectedError {
                message: "Buckets are empty.".to_string(),
                source: None,
            });
        }

        let cluster = self.metadata.get_cluster();
        let table_id = cluster.get_table(table_path).table_id;

        // Prepare requests
        let requests_by_server =
            self.prepare_list_offsets_requests(table_id, None, buckets_id, offset_spec)?;

        // Send Requests
        let response_futures = self.send_list_offsets_request(requests_by_server).await?;

        let mut results = HashMap::new();

        for response_future in response_futures {
            let offsets = response_future.await.map_err(|e| Error::UnexpectedError {
                message: "Fail to get result for list offsets.".to_string(),
                source: Some(Box::new(e)),
            })?;
            results.extend(offsets?);
        }
        Ok(results)
    }

    fn prepare_list_offsets_requests(
        &self,
        table_id: i64,
        partition_id: Option<i64>,
        buckets: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, ListOffsetsRequest>> {
        let cluster = self.metadata.get_cluster();
        let mut node_for_bucket_list: HashMap<i32, Vec<i32>> = HashMap::new();

        for bucket_id in buckets {
            let table_bucket = TableBucket::new(table_id, *bucket_id);
            let leader = cluster.leader_for(&table_bucket).ok_or_else(|| {
                // todo: consider retry?
                Error::UnexpectedError {
                    message: format!("No leader found for table bucket: {table_bucket}."),
                    source: None,
                }
            })?;

            node_for_bucket_list
                .entry(leader.id())
                .or_default()
                .push(*bucket_id);
        }

        let mut list_offsets_requests = HashMap::new();
        for (leader_id, bucket_ids) in node_for_bucket_list {
            let request =
                ListOffsetsRequest::new(table_id, partition_id, bucket_ids, offset_spec.clone());
            list_offsets_requests.insert(leader_id, request);
        }
        Ok(list_offsets_requests)
    }

    async fn send_list_offsets_request(
        &self,
        request_map: HashMap<i32, ListOffsetsRequest>,
    ) -> Result<Vec<JoinHandle<Result<HashMap<i32, i64>>>>> {
        let mut tasks = Vec::new();

        for (leader_id, request) in request_map {
            let rpc_client = self.rpc_client.clone();
            let metadata = self.metadata.clone();

            let task = tokio::spawn(async move {
                let cluster = metadata.get_cluster();
                let tablet_server = cluster.get_tablet_server(leader_id).ok_or_else(|| {
                    Error::LeaderNotAvailable {
                        message: format!(
                            "Tablet server {leader_id} is not found in metadata cache."
                        ),
                    }
                })?;
                let connection = rpc_client.get_connection(tablet_server).await?;
                let list_offsets_response = connection.request(request).await?;
                list_offsets_response.offsets()
            });
            tasks.push(task);
        }
        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ServerNode, ServerType};
    use crate::metadata::{
        DataField, DataTypes, DatabaseDescriptor, JsonSerde, Schema, TableDescriptor, TablePath,
    };
    use crate::proto::{
        CreateDatabaseResponse, CreateTableResponse, DatabaseExistsResponse, DropDatabaseResponse,
        DropTableResponse, GetDatabaseInfoResponse, GetLatestLakeSnapshotResponse,
        GetTableInfoResponse, ListDatabasesResponse, ListOffsetsResponse, ListTablesResponse,
        PbLakeSnapshotForBucket, PbListOffsetsRespForBucket, TableExistsResponse,
    };
    use crate::test_utils::{build_cluster_with_coordinator_arc, build_mock_connection};
    use prost::Message;
    use std::sync::Arc;

    const API_CREATE_DATABASE: i16 = 1001;
    const API_DROP_DATABASE: i16 = 1002;
    const API_LIST_DATABASES: i16 = 1003;
    const API_DATABASE_EXISTS: i16 = 1004;
    const API_CREATE_TABLE: i16 = 1005;
    const API_DROP_TABLE: i16 = 1006;
    const API_GET_TABLE: i16 = 1007;
    const API_LIST_TABLES: i16 = 1008;
    const API_TABLE_EXISTS: i16 = 1010;
    const API_LIST_OFFSETS: i16 = 1021;
    const API_GET_LAKE_SNAPSHOT: i16 = 1032;
    const API_GET_DATABASE_INFO: i16 = 1035;

    fn build_table_descriptor() -> TableDescriptor {
        let row_type = DataTypes::row(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let mut schema_builder = Schema::builder().with_row_type(&row_type);
        let schema = schema_builder.build().expect("schema");
        TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor")
    }

    #[tokio::test]
    async fn admin_requests_round_trip() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_id = 42;
        let table_descriptor = build_table_descriptor();
        let table_json =
            serde_json::to_vec(&table_descriptor.serialize_json().expect("table json")).unwrap();

        let db_descriptor = DatabaseDescriptor::builder()
            .comment("test")
            .custom_property("k", "v")
            .build();
        let db_json =
            serde_json::to_vec(&db_descriptor.serialize_json().expect("db json")).unwrap();

        let (admin_connection, admin_handle) =
            build_mock_connection(move |api_key: crate::rpc::ApiKey, _, _| {
                match i16::from(api_key) {
                    API_CREATE_DATABASE => CreateDatabaseResponse::default().encode_to_vec(),
                    API_CREATE_TABLE => CreateTableResponse::default().encode_to_vec(),
                    API_DROP_TABLE => DropTableResponse::default().encode_to_vec(),
                    API_DROP_DATABASE => DropDatabaseResponse::default().encode_to_vec(),
                    API_LIST_TABLES => ListTablesResponse {
                        table_name: vec!["tbl".to_string()],
                    }
                    .encode_to_vec(),
                    API_TABLE_EXISTS => TableExistsResponse { exists: true }.encode_to_vec(),
                    API_LIST_DATABASES => ListDatabasesResponse {
                        database_name: vec!["db".to_string(), "db2".to_string()],
                    }
                    .encode_to_vec(),
                    API_DATABASE_EXISTS => DatabaseExistsResponse { exists: false }.encode_to_vec(),
                    API_GET_TABLE => GetTableInfoResponse {
                        table_id,
                        schema_id: 1,
                        table_json: table_json.clone(),
                        created_time: 10,
                        modified_time: 20,
                    }
                    .encode_to_vec(),
                    API_GET_DATABASE_INFO => GetDatabaseInfoResponse {
                        database_json: db_json.clone(),
                        created_time: 5,
                        modified_time: 6,
                    }
                    .encode_to_vec(),
                    API_GET_LAKE_SNAPSHOT => GetLatestLakeSnapshotResponse {
                        table_id,
                        snapshot_id: 99,
                        bucket_snapshots: vec![PbLakeSnapshotForBucket {
                            partition_id: None,
                            bucket_id: 0,
                            log_offset: Some(123),
                        }],
                    }
                    .encode_to_vec(),
                    _ => vec![],
                }
            })
            .await;

        let (tablet_connection, tablet_handle) = build_mock_connection(
            |api_key: crate::rpc::ApiKey, _, _| match i16::from(api_key) {
                API_LIST_OFFSETS => ListOffsetsResponse {
                    buckets_resp: vec![PbListOffsetsRespForBucket {
                        bucket_id: 0,
                        error_code: None,
                        error_message: None,
                        offset: Some(7),
                    }],
                }
                .encode_to_vec(),
                _ => vec![],
            },
        )
        .await;

        let coordinator = ServerNode::new(
            100,
            "127.0.0.1".to_string(),
            9999,
            ServerType::CoordinatorServer,
        );
        let tablet = ServerNode::new(1, "127.0.0.1".to_string(), 9998, ServerType::TabletServer);
        let cluster = build_cluster_with_coordinator_arc(
            &table_path,
            table_id,
            coordinator.clone(),
            tablet.clone(),
        );
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let rpc_client = Arc::new(RpcClient::new());
        rpc_client.insert_connection_for_test(&coordinator, admin_connection);
        rpc_client.insert_connection_for_test(&tablet, tablet_connection);

        let admin = FlussAdmin::new(rpc_client.clone(), metadata.clone()).await?;

        admin
            .create_database("db", true, Some(&db_descriptor))
            .await?;
        admin
            .create_table(&table_path, &table_descriptor, true)
            .await?;
        admin.drop_table(&table_path, true).await?;

        let tables = admin.list_tables("db").await?;
        assert_eq!(tables, vec!["tbl".to_string()]);

        let exists = admin.table_exists(&table_path).await?;
        assert!(exists);

        let dbs = admin.list_databases().await?;
        assert_eq!(dbs.len(), 2);

        let db_exists = admin.database_exists("db").await?;
        assert!(!db_exists);

        let table_info = admin.get_table(&table_path).await?;
        assert_eq!(table_info.table_id, table_id);

        let db_info = admin.get_database_info("db").await?;
        assert_eq!(db_info.database_name(), "db");

        let snapshot = admin.get_latest_lake_snapshot(&table_path).await?;
        assert_eq!(snapshot.snapshot_id(), 99);
        assert_eq!(
            snapshot
                .table_buckets_offset()
                .get(&TableBucket::new(table_id, 0)),
            Some(&123)
        );

        let offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await?;
        assert_eq!(offsets.get(&0), Some(&7));

        admin_handle.abort();
        tablet_handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn list_offsets_empty_buckets_error() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let (admin_connection, admin_handle) = build_mock_connection(
            |api_key: crate::rpc::ApiKey, _, _| match i16::from(api_key) {
                API_CREATE_DATABASE => CreateDatabaseResponse::default().encode_to_vec(),
                _ => vec![],
            },
        )
        .await;
        let coordinator = ServerNode::new(
            10,
            "127.0.0.1".to_string(),
            9999,
            ServerType::CoordinatorServer,
        );
        let tablet = ServerNode::new(11, "127.0.0.1".to_string(), 8081, ServerType::TabletServer);
        let cluster = build_cluster_with_coordinator_arc(&table_path, 1, coordinator, tablet);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let rpc_client = Arc::new(RpcClient::new());
        rpc_client.insert_connection_for_test(
            metadata.get_cluster().get_coordinator_server().unwrap(),
            admin_connection,
        );

        let admin = FlussAdmin::new(rpc_client, metadata).await?;

        let result = admin
            .list_offsets(&table_path, &[], OffsetSpec::Earliest)
            .await;
        assert!(matches!(result, Err(Error::UnexpectedError { .. })));
        admin_handle.abort();
        Ok(())
    }
}
