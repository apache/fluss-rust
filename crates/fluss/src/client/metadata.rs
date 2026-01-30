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
use crate::error::{Error, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TablePath};
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
        let socket_address =
            boot_strap
                .parse::<SocketAddr>()
                .map_err(|e| Error::IllegalArgument {
                    message: format!("Invalid bootstrap address '{boot_strap}': {e}"),
                })?;
        let server_node = ServerNode::new(
            -1,
            socket_address.ip().to_string(),
            socket_address.port() as u32,
            ServerType::CoordinatorServer,
        );
        let con = connections.get_connection(&server_node).await?;

        let response = con
            .request(UpdateMetadataRequest::new(
                &HashSet::default(),
                &HashSet::new(),
                vec![],
            ))
            .await?;
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

    pub fn invalidate_physical_table_meta(
        &self,
        physical_tables_to_invalid: &HashSet<PhysicalTablePath>,
    ) {
        let mut cluster_guard = self.cluster.write();
        let updated_cluster =
            cluster_guard.invalidate_physical_table_meta(physical_tables_to_invalid);
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

    pub async fn update_tables_metadata(
        &self,
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<i64>,
    ) -> Result<()> {
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

        let response = conn
            .request(UpdateMetadataRequest::new(
                table_paths,
                physical_table_paths,
                partition_ids,
            ))
            .await?;
        self.update(response).await?;
        Ok(())
    }

    pub async fn update_table_metadata(&self, table_path: &TablePath) -> Result<()> {
        self.update_tables_metadata(&HashSet::from([table_path]), &HashSet::new(), vec![])
            .await
    }

    pub async fn check_and_update_table_metadata(&self, table_paths: &[TablePath]) -> Result<()> {
        let cluster_binding = self.cluster.read().clone();
        let need_update_table_paths: HashSet<&TablePath> = table_paths
            .iter()
            .filter(|table_path| cluster_binding.opt_get_table(table_path).is_none())
            .collect();

        if !need_update_table_paths.is_empty() {
            self.update_tables_metadata(&need_update_table_paths, &HashSet::new(), vec![])
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

    const MAX_RETRY_TIMES: u8 = 3;

    pub async fn leader_for(
        &self,
        table_path: &TablePath,
        table_bucket: &TableBucket,
    ) -> Result<Option<ServerNode>> {
        let leader = self.get_leader_for(table_bucket);

        if leader.is_some() {
            Ok(leader)
        } else {
            for _ in 0..Self::MAX_RETRY_TIMES {
                if let Some(partition_id) = table_bucket.partition_id() {
                    self.update_tables_metadata(
                        &HashSet::from([table_path]),
                        &HashSet::new(),
                        vec![partition_id],
                    )
                    .await?;
                } else {
                    self.update_tables_metadata(
                        &HashSet::from([table_path]),
                        &HashSet::new(),
                        vec![],
                    )
                    .await?;
                }

                let cluster = self.cluster.read();
                let leader = cluster.leader_for(table_bucket);

                if leader.is_some() {
                    return Ok(leader.cloned());
                }
            }

            Ok(None)
        }
    }

    fn get_leader_for(&self, table_bucket: &TableBucket) -> Option<ServerNode> {
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
    use crate::metadata::{TableBucket, TablePath};
    use crate::test_utils::build_cluster_arc;

    #[tokio::test]
    async fn leader_for_returns_server() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Metadata::new_for_test(cluster);
        let leader = metadata
            .leader_for(&table_path, &TableBucket::new(1, 0))
            .await
            .expect("leader request should be Ok")
            .expect("leader should exist");
        assert_eq!(leader.id(), 1);
    }

    #[test]
    fn invalidate_server_removes_leader() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Metadata::new_for_test(cluster);
        metadata.invalidate_server(&1, vec![1]);
        let cluster = metadata.get_cluster();
        assert!(cluster.get_tablet_server(1).is_none());
    }
}
