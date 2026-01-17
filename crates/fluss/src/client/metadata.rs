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

use crate::cluster::{Cluster, ServerNode, ServerType};
use crate::error::Result;
use crate::metadata::{TableBucket, TablePath};
use crate::proto::MetadataResponse;
use crate::rpc::message::UpdateMetadataRequest;
use crate::rpc::{RpcClient, ServerConnection};
use log::info;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Default)]
pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
    connections: Arc<RpcClient>,
    bootstrap: Arc<str>,
}

impl Metadata {
    pub async fn new(bootstrap: &str, connections: Arc<RpcClient>) -> Result<Self> {
        let cluster = Self::init_cluster(bootstrap, connections.clone()).await?;
        Ok(Metadata {
            cluster: RwLock::new(Arc::new(cluster)),
            connections,
            bootstrap: bootstrap.into(),
        })
    }

    async fn init_cluster(boot_strap: &str, connections: Arc<RpcClient>) -> Result<Cluster> {
        let socket_address = boot_strap.parse::<SocketAddr>().unwrap();
        let server_node = ServerNode::new(
            -1,
            socket_address.ip().to_string(),
            socket_address.port() as u32,
            ServerType::CoordinatorServer,
        );
        let con = connections.get_connection(&server_node).await?;
        let response = con.request(UpdateMetadataRequest::new(&[])).await?;
        Cluster::from_metadata_response(response, None)
    }

    async fn reinit_cluster(&self) -> Result<()> {
        let cluster = Self::init_cluster(&self.bootstrap, self.connections.clone()).await?;
        *self.cluster.write() = cluster.into();
        Ok(())
    }

    pub fn invalidate_server(&self, server_id: &i32, table_ids: Vec<i64>) {
        // Take a write lock for the entire operation to avoid races between
        // reading the current cluster state and writing back the updated one.
        let mut cluster_guard = self.cluster.write();
        let updated_cluster = cluster_guard.invalidate_server(server_id, table_ids);
        *cluster_guard = Arc::new(updated_cluster);
    }

    pub async fn update(&self, metadata_response: MetadataResponse) -> Result<()> {
        let origin_cluster = self.cluster.read().clone();
        let new_cluster =
            Cluster::from_metadata_response(metadata_response, Some(&origin_cluster))?;
        let mut cluster = self.cluster.write();
        *cluster = Arc::new(new_cluster);
        Ok(())
    }

    pub async fn update_tables_metadata(&self, table_paths: &HashSet<&TablePath>) -> Result<()> {
        let maybe_server = {
            let guard = self.cluster.read();
            guard.get_one_available_server().cloned()
        };

        let server = match maybe_server {
            Some(s) => s,
            None => {
                info!(
                    "No available tablet server to update metadata, attempting to re-initialize cluster using bootstrap server."
                );
                self.reinit_cluster().await?;
                return Ok(());
            }
        };

        let conn = self.connections.get_connection(&server).await?;

        let update_table_paths: Vec<&TablePath> = table_paths.iter().copied().collect();
        let response = conn
            .request(UpdateMetadataRequest::new(update_table_paths.as_slice()))
            .await?;
        self.update(response).await?;
        Ok(())
    }

    pub async fn update_table_metadata(&self, table_path: &TablePath) -> Result<()> {
        self.update_tables_metadata(&HashSet::from([table_path]))
            .await
    }

    pub async fn check_and_update_table_metadata(&self, table_paths: &[TablePath]) -> Result<()> {
        let cluster_binding = self.cluster.read().clone();
        let need_update_table_paths: HashSet<&TablePath> = table_paths
            .iter()
            .filter(|table_path| cluster_binding.opt_get_table(table_path).is_none())
            .collect();
        if !need_update_table_paths.is_empty() {
            self.update_tables_metadata(&need_update_table_paths)
                .await?;
        }
        Ok(())
    }

    pub async fn get_connection(&self, server_node: &ServerNode) -> Result<ServerConnection> {
        let result = self.connections.get_connection(server_node).await?;
        Ok(result)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        let guard = self.cluster.read();
        guard.clone()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<ServerNode> {
        let cluster = self.cluster.read();
        cluster.leader_for(table_bucket).cloned()
    }
}

#[cfg(test)]
impl Metadata {
    pub(crate) fn new_for_test(cluster: Arc<Cluster>) -> Self {
        Metadata {
            cluster: RwLock::new(cluster),
            connections: Arc::new(RpcClient::new()),
            bootstrap: Arc::from(""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::metadata::{
        DataField, DataTypes, JsonSerde, Schema, TableBucket, TableDescriptor, TableInfo, TablePath,
    };
    use crate::proto::{
        MetadataResponse, PbBucketMetadata, PbServerNode, PbTableMetadata, PbTablePath,
    };
    use crate::test_utils::build_mock_connection;
    use prost::Message;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    const API_UPDATE_METADATA: i16 = 1012;

    fn build_table_info(table_path: TablePath, table_id: i64) -> TableInfo {
        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let mut schema_builder = Schema::builder().with_row_type(&row_type);
        let schema = schema_builder.build().expect("schema build");
        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor build");
        TableInfo::of(table_path, table_id, 1, table_descriptor, 0, 0)
    }

    fn build_cluster(table_path: &TablePath, table_id: i64) -> Arc<Cluster> {
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let table_bucket = TableBucket::new(table_id, 0);
        let bucket_location = BucketLocation::new(
            table_bucket.clone(),
            Some(server.clone()),
            table_path.clone(),
        );

        let mut servers = HashMap::new();
        servers.insert(server.id(), server);

        let mut locations_by_path = HashMap::new();
        locations_by_path.insert(table_path.clone(), vec![bucket_location.clone()]);

        let mut locations_by_bucket = HashMap::new();
        locations_by_bucket.insert(table_bucket, bucket_location);

        let mut table_id_by_path = HashMap::new();
        table_id_by_path.insert(table_path.clone(), table_id);

        let mut table_info_by_path = HashMap::new();
        table_info_by_path.insert(
            table_path.clone(),
            build_table_info(table_path.clone(), table_id),
        );

        Arc::new(Cluster::new(
            None,
            servers,
            locations_by_path,
            locations_by_bucket,
            table_id_by_path,
            table_info_by_path,
        ))
    }

    fn build_cluster_with_server(server: ServerNode) -> Arc<Cluster> {
        Arc::new(Cluster::new(
            None,
            HashMap::from([(server.id(), server)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    fn build_metadata_response(table_path: &TablePath, table_id: i64) -> MetadataResponse {
        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let mut schema_builder = Schema::builder().with_row_type(&row_type);
        let schema = schema_builder.build().expect("schema build");
        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor build");
        let table_json =
            serde_json::to_vec(&table_descriptor.serialize_json().expect("table json")).unwrap();

        MetadataResponse {
            coordinator_server: Some(PbServerNode {
                node_id: 10,
                host: "127.0.0.1".to_string(),
                port: 9999,
                listeners: None,
            }),
            tablet_servers: vec![PbServerNode {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: 9092,
                listeners: None,
            }],
            table_metadata: vec![PbTableMetadata {
                table_path: PbTablePath {
                    database_name: table_path.database().to_string(),
                    table_name: table_path.table().to_string(),
                },
                table_id,
                schema_id: 1,
                table_json,
                bucket_metadata: vec![PbBucketMetadata {
                    bucket_id: 0,
                    leader_id: Some(1),
                    replica_id: vec![1],
                }],
                created_time: 0,
                modified_time: 0,
            }],
            partition_metadata: vec![],
        }
    }

    #[test]
    fn leader_for_returns_server() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1);
        let metadata = Metadata::new_for_test(cluster);
        let leader = metadata
            .leader_for(&TableBucket::new(1, 0))
            .expect("leader");
        assert_eq!(leader.id(), 1);
    }

    #[test]
    fn invalidate_server_removes_leader() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1);
        let metadata = Metadata::new_for_test(cluster);
        metadata.invalidate_server(&1, vec![1]);
        let cluster = metadata.get_cluster();
        assert!(cluster.get_tablet_server(1).is_none());
    }

    #[tokio::test]
    async fn update_replaces_cluster_state() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let metadata = Metadata::new_for_test(Arc::new(Cluster::default()));

        let response = build_metadata_response(&table_path, 1);
        metadata.update(response).await?;

        let cluster = metadata.get_cluster();
        assert!(cluster.get_tablet_server(1).is_some());
        assert!(cluster.opt_get_table(&table_path).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn check_and_update_table_metadata_noop_when_present() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1);
        let metadata = Metadata::new_for_test(cluster);
        metadata
            .check_and_update_table_metadata(&[table_path.clone()])
            .await?;
        let cluster = metadata.get_cluster();
        assert!(cluster.opt_get_table(&table_path).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn update_tables_metadata_refreshes_cluster() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let metadata = Metadata::new_for_test(build_cluster_with_server(server.clone()));
        let response = build_metadata_response(&table_path, 1);
        let response_bytes = response.encode_to_vec();
        let (connection, handle) =
            build_mock_connection(move |api_key, _, _| match i16::from(api_key) {
                API_UPDATE_METADATA => response_bytes.clone(),
                _ => vec![],
            })
            .await;
        metadata
            .connections
            .insert_connection_for_test(&server, connection);

        metadata
            .update_tables_metadata(&HashSet::from([&table_path]))
            .await?;
        assert!(metadata.get_cluster().opt_get_table(&table_path).is_some());
        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn check_and_update_table_metadata_triggers_update() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let metadata = Metadata::new_for_test(build_cluster_with_server(server.clone()));
        let response = build_metadata_response(&table_path, 1);
        let response_bytes = response.encode_to_vec();
        let (connection, handle) =
            build_mock_connection(move |api_key, _, _| match i16::from(api_key) {
                API_UPDATE_METADATA => response_bytes.clone(),
                _ => vec![],
            })
            .await;
        metadata
            .connections
            .insert_connection_for_test(&server, connection);

        metadata
            .check_and_update_table_metadata(&[table_path.clone()])
            .await?;
        assert!(metadata.get_cluster().opt_get_table(&table_path).is_some());
        handle.abort();
        Ok(())
    }
}
