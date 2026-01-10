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

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::LazyLock;

use crate::integration::fluss_cluster::FlussTestingCluster;
#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: LazyLock<Arc<RwLock<Option<FlussTestingCluster>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod table_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::create_table;
    use arrow::array::record_batch;
    use fluss::client::{FlussTable, TableScan};
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss::record::ScanRecord;
    use fluss::row::InternalRow;
    use fluss::rpc::message::OffsetSpec;
    use jiff::Timestamp;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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

        // Create scanner to verify appended records
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
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

        // Poll for records
        let scan_records = log_scanner
            .poll(tokio::time::Duration::from_secs(10))
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

    #[tokio::test]
    async fn list_offsets() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_list_offsets".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        // Wait for table to be fully initialized
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Test earliest offset (should be 0 for empty table)
        let earliest_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await
            .expect("Failed to list earliest offsets");

        assert_eq!(
            earliest_offsets.get(&0),
            Some(&0),
            "Earliest offset should be 0 for bucket 0"
        );

        // Test latest offset (should be 0 for empty table)
        let latest_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list latest offsets");

        assert_eq!(
            latest_offsets.get(&0),
            Some(&0),
            "Latest offset should be 0 for empty table"
        );

        let before_append_ms = Timestamp::now().as_millisecond();

        // Append some records
        let append_writer = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table")
            .new_append()
            .expect("Failed to create append")
            .create_writer();

        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["alice", "bob", "charlie"])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
            .await
            .expect("Failed to append batch");

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let after_append_ms = Timestamp::now().as_millisecond();

        // Test latest offset after appending (should be 3)
        let latest_offsets_after = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list latest offsets after append");

        assert_eq!(
            latest_offsets_after.get(&0),
            Some(&3),
            "Latest offset should be 3 after appending 3 records"
        );

        // Test earliest offset after appending (should still be 0)
        let earliest_offsets_after = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await
            .expect("Failed to list earliest offsets after append");

        assert_eq!(
            earliest_offsets_after.get(&0),
            Some(&0),
            "Earliest offset should still be 0"
        );

        // Test list_offsets_by_timestamp

        let timestamp_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(before_append_ms))
            .await
            .expect("Failed to list offsets by timestamp");

        assert_eq!(
            timestamp_offsets.get(&0),
            Some(&0),
            "Timestamp before append should resolve to offset 0 (start of new data)"
        );

        let timestamp_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(after_append_ms))
            .await
            .expect("Failed to list offsets by timestamp");

        assert_eq!(
            timestamp_offsets.get(&0),
            Some(&3),
            "Timestamp after append should resolve to offset 0 (no newer records)"
        );
    }

    #[tokio::test]
    async fn test_project() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_project".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("col_a", DataTypes::int())
                    .column("col_b", DataTypes::string())
                    .column("col_c", DataTypes::int())
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

        // Append 3 records
        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer();

        let batch = record_batch!(
            ("col_a", Int32, [1, 2, 3]),
            ("col_b", Utf8, ["x", "y", "z"]),
            ("col_c", Int32, [10, 20, 30])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
            .await
            .expect("Failed to append batch");
        append_writer.flush().await.expect("Failed to flush");

        // Test project_by_name: select col_b and col_c only
        let records = scan_table(&table, |scan| {
            scan.project_by_name(&["col_b", "col_c"])
                .expect("Failed to project by name")
        })
        .await;

        assert_eq!(
            records.len(),
            3,
            "Should have 3 records with project_by_name"
        );

        // Verify projected columns are in the correct order (col_b, col_c)
        let expected_col_b = ["x", "y", "z"];
        let expected_col_c = [10, 20, 30];

        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            // col_b is now at index 0, col_c is at index 1
            assert_eq!(
                row.get_string(0),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1),
                expected_col_c[i],
                "col_c mismatch at index {}",
                i
            );
        }

        // test project by column indices
        let records = scan_table(&table, |scan| {
            scan.project(&[1, 0]).expect("Failed to project by indices")
        })
        .await;

        assert_eq!(
            records.len(),
            3,
            "Should have 3 records with project_by_name"
        );
        // Verify projected columns are in the correct order (col_b, col_a)
        let expected_col_b = ["x", "y", "z"];
        let expected_col_a = [1, 2, 3];

        for (i, record) in records.iter().enumerate() {
            let row = record.row();
            // col_b is now at index 0, col_c is at index 1
            assert_eq!(
                row.get_string(0),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1),
                expected_col_a[i],
                "col_c mismatch at index {}",
                i
            );
        }

        // Test error case: empty column names should fail
        let result = table.new_scan().project_by_name(&[]);
        assert!(
            result.is_err(),
            "project_by_name with empty names should fail"
        );

        // Test error case: non-existent column should fail
        let result = table.new_scan().project_by_name(&["nonexistent_column"]);
        assert!(
            result.is_err(),
            "project_by_name with non-existent column should fail"
        );
    }

    async fn scan_table<'a>(
        table: &FlussTable<'a>,
        setup_scan: impl FnOnce(TableScan) -> TableScan,
    ) -> Vec<ScanRecord> {
        // 1. build log scanner
        let log_scanner = setup_scan(table.new_scan())
            .create_log_scanner()
            .expect("Failed to create log scanner");

        // 2. subscribe
        let mut bucket_offsets = HashMap::new();
        bucket_offsets.insert(0, 0);
        log_scanner
            .subscribe_batch(&bucket_offsets)
            .await
            .expect("Failed to subscribe");

        // 3. poll records
        let scan_records = log_scanner
            .poll(Duration::from_secs(10))
            .await
            .expect("Failed to poll");

        // 4. collect and sort
        let mut records: Vec<_> = scan_records.into_iter().collect();
        records.sort_by_key(|r| r.offset());
        records
    }

    #[tokio::test]
    async fn test_poll_batches() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_poll_batches".to_string());
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();

        create_table(
            &admin,
            &table_path,
            &TableDescriptor::builder().schema(schema).build().unwrap(),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let table = connection.get_table(&table_path).await.unwrap();
        let scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        scanner.subscribe(0, 0).await.unwrap();

        // Test 1: Empty table should return empty result
        assert!(
            scanner
                .poll(Duration::from_millis(500))
                .await
                .unwrap()
                .is_empty()
        );

        let writer = table.new_append().unwrap().create_writer();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [1, 2]), ("name", Utf8, ["a", "b"])).unwrap(),
            )
            .await
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [3, 4]), ("name", Utf8, ["c", "d"])).unwrap(),
            )
            .await
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [5, 6]), ("name", Utf8, ["e", "f"])).unwrap(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        use arrow::array::Int32Array;
        let batches = scanner.poll(Duration::from_secs(10)).await.unwrap();
        let mut all_ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(i)
                })
            })
            .collect();

        // Test 2: Order should be preserved across multiple batches
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5, 6]);

        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [7, 8]), ("name", Utf8, ["g", "h"])).unwrap(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let more = scanner.poll(Duration::from_secs(10)).await.unwrap();
        let new_ids: Vec<i32> = more
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(i)
                })
            })
            .collect();

        // Test 3: Subsequent polls should not return duplicate data (offset continuation)
        assert_eq!(new_ids, vec![7, 8]);

        // Test 4: Subscribing from mid-offset should truncate batch (Arrow batch slicing)
        // Server returns all records from start of batch, but client truncates to subscription offset
        let trunc_scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        trunc_scanner.subscribe(0, 3).await.unwrap();
        let trunc_batches = trunc_scanner.poll(Duration::from_secs(10)).await.unwrap();
        let trunc_ids: Vec<i32> = trunc_batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(i)
                })
            })
            .collect();

        // Subscribing from offset 3 should return [4,5,6,7,8], not [1,2,3,4,5,6,7,8]
        assert_eq!(trunc_ids, vec![4, 5, 6, 7, 8]);

        // Test 5: Projection should only return requested columns
        let proj = table
            .new_scan()
            .project_by_name(&["id"])
            .unwrap()
            .create_record_batch_log_scanner()
            .unwrap();
        proj.subscribe(0, 0).await.unwrap();
        let proj_batches = proj.poll(Duration::from_secs(10)).await.unwrap();

        // Projected batch should have 1 column (id), not 2 (id, name)
        assert_eq!(proj_batches[0].num_columns(), 1);
    }
}
