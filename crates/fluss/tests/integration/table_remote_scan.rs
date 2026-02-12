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
use crate::integration::fluss_cluster::FlussTestingCluster;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::LazyLock;

#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: LazyLock<Arc<RwLock<Option<FlussTestingCluster>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod table_remote_scan_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::{
        create_table, get_cluster, stop_cluster, wait_for_cluster_ready,
    };
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::{GenericRow, InternalRow};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;
    fn before_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_lock = SHARED_FLUSS_CLUSTER.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                // Create a temporary directory for remote data that can be accessed from both
                // container and host. Use a fixed path so it's the same in container and host.
                // On macOS, Docker Desktop may have issues with /tmp, so we use a path in the
                // current working directory or user's home directory which Docker can access.
                let temp_dir = std::env::current_dir()
                    .unwrap_or_else(|_| std::path::PathBuf::from("."))
                    .join("target")
                    .join(format!("test-remote-data-{}", Uuid::new_v4()));

                // Remove existing directory if it exists to start fresh
                let _ = std::fs::remove_dir_all(&temp_dir);
                std::fs::create_dir_all(&temp_dir)
                    .expect("Failed to create temporary directory for remote data");
                println!("temp_dir: {:?}", temp_dir);

                // Verify directory was created and is accessible
                if !temp_dir.exists() {
                    panic!("Remote data directory was not created: {:?}", temp_dir);
                }

                // Get absolute path for Docker mount
                let temp_dir = temp_dir
                    .canonicalize()
                    .expect("Failed to canonicalize remote data directory path");

                let mut cluster_conf = HashMap::new();
                // set to a small size to make data can be tiered to remote
                cluster_conf.insert("log.segment.file-size".to_string(), "120b".to_string());
                cluster_conf.insert(
                    "remote.log.task-interval-duration".to_string(),
                    "1s".to_string(),
                );
                // remote.data.dir uses the same path in container and host
                cluster_conf.insert(
                    "remote.data.dir".to_string(),
                    temp_dir.to_string_lossy().to_string(),
                );

                let cluster = FlussTestingClusterBuilder::new_with_cluster_conf(
                    "test_table_remote",
                    &cluster_conf,
                )
                .with_remote_data_dir(temp_dir)
                .build()
                .await;
                wait_for_cluster_ready(&cluster).await;
                let mut guard = cluster_lock.write();
                *guard = Some(cluster);
            });
        })
        .join()
        .expect("Failed to create cluster");
    }

    fn after_all() {
        stop_cluster(SHARED_FLUSS_CLUSTER.clone());
    }

    #[tokio::test]
    async fn test_scan_remote_log() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_append_record_batch_and_scan");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // append 20 rows, there must be some tiered to remote
        let record_count = 20;
        for i in 0..record_count {
            let mut row = GenericRow::new(2);
            row.set_field(0, i as i32);
            let v = format!("v{}", i);
            row.set_field(1, v.as_str());
            append_writer.append(&row).expect("Failed to append row");
        }

        append_writer.flush().await.expect("Failed to flush");

        // Create a log scanner and subscribe to all buckets to read appended records
        let num_buckets = table.get_table_info().get_num_buckets();
        let log_scanner = table
            .new_scan()
            .project(&[1, 0])
            .unwrap()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, 0)
                .await
                .expect("Failed to subscribe");
        }

        let mut records = Vec::with_capacity(record_count);
        let start = std::time::Instant::now();
        const MAX_WAIT_DURATION: Duration = Duration::from_secs(60);
        while records.len() < record_count {
            if start.elapsed() > MAX_WAIT_DURATION {
                panic!(
                    "Timed out waiting for {} records; only got {} after {:?}",
                    record_count,
                    records.len(),
                    start.elapsed()
                );
            }
            let scan_records = log_scanner
                .poll(Duration::from_secs(1))
                .await
                .expect("Failed to poll log scanner");
            records.extend(scan_records);
        }

        // then, check the data
        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            let expected_c1 = i as i32;
            let expected_c2 = format!("v{}", i);
            assert_eq!(row.get_int(1), expected_c1, "c1 mismatch at index {}", i);
            assert_eq!(row.get_string(0), expected_c2, "c2 mismatch at index {}", i);
        }
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        get_cluster(&SHARED_FLUSS_CLUSTER)
    }
}
