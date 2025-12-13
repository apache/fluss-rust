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

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::integration::fluss_cluster::FlussTestingCluster;
#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: Lazy<Arc<RwLock<Option<FlussTestingCluster>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod table_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::create_table;
    use arrow::array::record_batch;
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss::row::InternalRow;
    use std::sync::Arc;
    use std::thread;
    fn before_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let cluster = FlussTestingClusterBuilder::new("test_table").build().await;
                let mut guard = cluster_guard.write();
                *guard = Some(cluster);
            });
        })
        .join()
        .expect("Failed to create cluster");

        // wait for 20 seconds to avoid the error like
        // CoordinatorEventProcessor is not initialized yet
        thread::sleep(std::time::Duration::from_secs(20));
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        let cluster_guard = SHARED_FLUSS_CLUSTER.read();
        if cluster_guard.is_none() {
            panic!("Fluss cluster not initialized. Make sure before_all() was called.");
        }
        Arc::new(cluster_guard.as_ref().unwrap().clone())
    }

    fn after_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let mut guard = cluster_guard.write();
                if let Some(cluster) = guard.take() {
                    cluster.stop().await;
                }
            });
        })
        .join()
        .expect("Failed to cleanup cluster");
    }

    #[tokio::test]
    async fn append_record_batch_and_scan() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new(
            "fluss".to_string(),
            "test_append_record_batch_and_scan".to_string(),
        );

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .property("table.log.arrow.compression.type", "NONE")
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
            .create_writer();

        let batch1 =
            record_batch!(("c1", Int32, [1, 2, 3]), ("c2", Utf8, ["a1", "a2", "a3"])).unwrap();
        append_writer
            .append_arrow_batch(batch1)
            .await
            .expect("Failed to append batch");

        let batch2 =
            record_batch!(("c1", Int32, [4, 5, 6]), ("c2", Utf8, ["a4", "a5", "a6"])).unwrap();
        append_writer
            .append_arrow_batch(batch2)
            .await
            .expect("Failed to append batch");

        append_writer.flush().await.expect("Failed to flush");

        let num_buckets = table.table_info().get_num_buckets();
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, 0)
                .await
                .expect("Failed to subscribe");
        }

        let scan_records = log_scanner
            .poll(std::time::Duration::from_secs(5))
            .await
            .expect("Failed to poll");

        let mut records: Vec<_> = scan_records.into_iter().collect();
        records.sort_by_key(|r| r.offset());

        assert_eq!(records.len(), 6, "Should have 6 records");
        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            let expected_c1 = (i + 1) as i32;
            let expected_c2 = format!("a{}", i + 1);
            assert_eq!(row.get_int(0), expected_c1, "c1 mismatch at index {}", i);
            assert_eq!(row.get_string(1), expected_c2, "c2 mismatch at index {}", i);
        }

        let log_scanner_projected = table
            .new_scan()
            .project(&[1, 0])
            .expect("Failed to project")
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner_projected
                .subscribe(bucket_id, 0)
                .await
                .expect("Failed to subscribe");
        }

        let scan_records_projected = log_scanner_projected
            .poll(std::time::Duration::from_secs(5))
            .await
            .expect("Failed to poll");

        let mut records_projected: Vec<_> = scan_records_projected.into_iter().collect();
        records_projected.sort_by_key(|r| r.offset());

        assert_eq!(
            records_projected.len(),
            6,
            "Should have 6 records with projection"
        );
        for (i, record) in records_projected.iter().enumerate() {
            let row = record.row();
            let expected_c2 = format!("a{}", i + 1);
            let expected_c1 = (i + 1) as i32;
            assert_eq!(
                row.get_string(0),
                expected_c2,
                "Projected c2 (first column) mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1),
                expected_c1,
                "Projected c1 (second column) mismatch at index {}",
                i
            );
        }

        // Create scanner to verify appended records
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let table_scan = table.new_scan();
        let log_scanner = table_scan
            .create_log_scanner()
            .expect("Failed to create log scanner");

        // Subscribe to bucket 0 starting from offset 0
        log_scanner
            .subscribe(0, 0)
            .await
            .expect("Failed to subscribe to bucket");

        // Poll for records
        let scan_records = log_scanner
            .poll(tokio::time::Duration::from_secs(5))
            .await
            .expect("Failed to poll records");

        // Verify the scanned records
        let table_bucket = TableBucket::new(table.table_info().table_id, 0);
        let records = scan_records.records(&table_bucket);

        assert_eq!(records.len(), 6, "Expected 6 records");

        // Verify record contents match what was appended
        let expected_c1_values = vec![1, 2, 3, 4, 5, 6];
        let expected_c2_values = vec!["a1", "a2", "a3", "a4", "a5", "a6"];

        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            assert_eq!(
                row.get_int(0),
                expected_c1_values[i],
                "c1 value mismatch at row {}",
                i
            );
            assert_eq!(
                row.get_string(1),
                expected_c2_values[i],
                "c2 value mismatch at row {}",
                i
            );
        }
    }
}
