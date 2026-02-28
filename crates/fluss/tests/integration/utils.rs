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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

extern "C" fn cleanup_on_exit() {
    SHARED_CLUSTER.stop();
}

/// Shared cluster with dual listeners: PLAIN_CLIENT (plaintext) on port 9223
/// and CLIENT (SASL) on port 9123. Includes remote storage config so
/// table_remote_scan can also use this cluster.
static SHARED_CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let temp_dir = std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join("target")
                .join(format!("test-remote-data-{}", uuid::Uuid::new_v4()));
            let _ = std::fs::remove_dir_all(&temp_dir);
            std::fs::create_dir_all(&temp_dir)
                .expect("Failed to create temporary directory for remote data");
            let temp_dir = temp_dir
                .canonicalize()
                .expect("Failed to canonicalize remote data directory path");

            let mut cluster_conf = HashMap::new();
            cluster_conf.insert("log.segment.file-size".to_string(), "120b".to_string());
            cluster_conf.insert(
                "remote.log.task-interval-duration".to_string(),
                "1s".to_string(),
            );

            let cluster =
                FlussTestingClusterBuilder::new_with_cluster_conf("shared-test", &cluster_conf)
                    .with_sasl(vec![
                        ("admin".to_string(), "admin-secret".to_string()),
                        ("alice".to_string(), "alice-secret".to_string()),
                    ])
                    .with_remote_data_dir(temp_dir)
                    .build()
                    .await;
            wait_for_cluster_ready_with_sasl(&cluster).await;

            // Register cleanup so containers are removed on process exit.
            unsafe {
                unsafe extern "C" {
                    fn atexit(f: extern "C" fn()) -> std::os::raw::c_int;
                }
                atexit(cleanup_on_exit);
            }

            cluster
        })
    })
    .join()
    .expect("Failed to initialize shared cluster")
});

/// Returns an `Arc` to the shared test cluster.
pub fn get_shared_cluster() -> Arc<FlussTestingCluster> {
    Arc::new(SHARED_CLUSTER.clone())
}

pub async fn create_table(
    admin: &FlussAdmin,
    table_path: &TablePath,
    table_descriptor: &TableDescriptor,
) {
    admin
        .create_table(table_path, table_descriptor, false)
        .await
        .expect("Failed to create table");
}

/// Similar to wait_for_cluster_ready but connects with SASL credentials.
pub async fn wait_for_cluster_ready_with_sasl(cluster: &FlussTestingCluster) {
    let timeout = Duration::from_secs(30);
    let poll_interval = Duration::from_millis(500);
    let start = std::time::Instant::now();

    let (username, password) = cluster
        .sasl_users()
        .first()
        .expect("SASL cluster must have at least one user");

    loop {
        let connection = cluster
            .get_fluss_connection_with_sasl(username, password)
            .await;
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
                "SASL server readiness check timed out after {} seconds. \
                 CoordinatorEventProcessor may not be initialized or TabletServer may not be available.",
                timeout.as_secs()
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
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
        partition_map.insert(partition_column, *value);
        admin
            .create_partition(table_path, &PartitionSpec::new(partition_map), true)
            .await
            .expect("Failed to create partition");
    }
}
