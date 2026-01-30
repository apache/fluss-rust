/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use fluss::client::FlussAdmin;
use fluss::metadata::{PartitionSpec, TableDescriptor, TablePath};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Polls the cluster until CoordinatorEventProcessor is initialized and tablet server is available.
/// Times out after 20 seconds.
pub async fn wait_for_cluster_ready(cluster: &FlussTestingCluster) {
    let timeout = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();

    loop {
        let connection = cluster.get_fluss_connection().await;
        if connection.get_admin().await.is_ok()
            && connection
                .get_metadata()
                .get_cluster()
                .get_one_available_server()
                .is_some()
        {
            return;
        }

        if start.elapsed() >= timeout {
            panic!(
                "Server readiness check timed out after {} seconds. \
                 CoordinatorEventProcessor may not be initialized or TabletServer may not be available.",
                timeout.as_secs()
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn create_table(
    admin: &FlussAdmin,
    table_path: &TablePath,
    table_descriptor: &TableDescriptor,
) {
    admin
        .create_table(&table_path, &table_descriptor, false)
        .await
        .expect("Failed to create table");
}

pub fn start_cluster(name: &str, cluster_lock: Arc<RwLock<Option<FlussTestingCluster>>>) {
    let name = name.to_string();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let cluster = FlussTestingClusterBuilder::new(&name).build().await;
            wait_for_cluster_ready(&cluster).await;
            let mut guard = cluster_lock.write();
            *guard = Some(cluster);
        });
    })
    .join()
    .expect("Failed to create cluster");
}

pub fn stop_cluster(cluster_lock: Arc<RwLock<Option<FlussTestingCluster>>>) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut guard = cluster_lock.write();
            if let Some(cluster) = guard.take() {
                cluster.stop().await;
            }
        });
    })
    .join()
    .expect("Failed to cleanup cluster");
}

pub fn get_cluster(cluster_lock: &RwLock<Option<FlussTestingCluster>>) -> Arc<FlussTestingCluster> {
    let guard = cluster_lock.read();
    Arc::new(
        guard
            .as_ref()
            .expect("Fluss cluster not initialized. Make sure before_all() was called.")
            .clone(),
    )
}

/// Creates partitions for a partitioned table.
///
/// # Arguments
/// * `admin` - The FlussAdmin instance
/// * `table_path` - The table path
/// * `partition_column` - The partition column name
/// * `partition_values` - The partition values to create
pub async fn create_partitions(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_column: &str,
    partition_values: &[&str],
) {
    for value in partition_values {
        let mut partition_map = HashMap::new();
        partition_map.insert(partition_column.to_string(), value.to_string());
        admin
            .create_partition(table_path, &PartitionSpec::new(partition_map), true)
            .await
            .expect("Failed to create partition");
    }
}
