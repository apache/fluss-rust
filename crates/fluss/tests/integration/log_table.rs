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

#[cfg(test)]
mod table_test {
    use crate::integration::utils::{create_partitions, create_table, get_shared_cluster};
    use arrow::array::record_batch;
    use fluss::client::{EARLIEST_OFFSET, FlussTable, TableScan};
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::ScanRecord;
    use fluss::row::InternalRow;
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn append_record_batch_and_scan() {
        let cluster = get_shared_cluster();
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
            .distributed_by(Some(3), vec!["c1".to_string()])
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

        let batch1 =
            record_batch!(("c1", Int32, [1, 2, 3]), ("c2", Utf8, ["a1", "a2", "a3"])).unwrap();
        append_writer
            .append_arrow_batch(batch1)
            .expect("Failed to append batch");

        let batch2 =
            record_batch!(("c1", Int32, [4, 5, 6]), ("c2", Utf8, ["a4", "a5", "a6"])).unwrap();
        append_writer
            .append_arrow_batch(batch2)
            .expect("Failed to append batch");

        // Flush to ensure all writes are acknowledged
        append_writer.flush().await.expect("Failed to flush");

        // Create scanner to verify appended records
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let num_buckets = table.get_table_info().get_num_buckets();
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        for bucket_id in 0..num_buckets {
            log_scanner
                .subscribe(bucket_id, EARLIEST_OFFSET)
                .await
                .expect("Failed to subscribe with EARLIEST_OFFSET");
        }

        // Poll for records across all buckets
        let mut collected: Vec<(i32, String)> = Vec::new();
        let start_time = std::time::Instant::now();
        while collected.len() < 6 && start_time.elapsed() < Duration::from_secs(10) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll records");
            for rec in scan_records {
                let row = rec.row();
                collected.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                ));
            }
        }

        assert_eq!(collected.len(), 6, "Expected 6 records");

        // Sort and verify record contents
        collected.sort();
        let expected: Vec<(i32, String)> = vec![
            (1, "a1".to_string()),
            (2, "a2".to_string()),
            (3, "a3".to_string()),
            (4, "a4".to_string()),
            (5, "a5".to_string()),
            (6, "a6".to_string()),
        ];
        assert_eq!(collected, expected);

        // Test unsubscribe: unsubscribe from bucket 0, verify no error
        log_scanner
            .unsubscribe(0)
            .await
            .expect("Failed to unsubscribe from bucket 0");

        // Verify unsubscribe_partition fails on a non-partitioned table
        assert!(
            log_scanner.unsubscribe_partition(0, 0).await.is_err(),
            "unsubscribe_partition should fail on a non-partitioned table"
        );
    }

    #[tokio::test]
    async fn list_offsets() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_list_offsets");

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

        // Append some records
        let append_writer = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table")
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["alice", "bob", "charlie"])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
            .expect("Failed to append batch");

        // Flush to ensure all writes are acknowledged
        append_writer.flush().await.expect("Failed to flush");

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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

        // Scan records back to get server-assigned timestamps (avoids host/container
        // clock skew issues that make host-based timestamps unreliable).
        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("Failed to subscribe");

        let mut record_timestamps: Vec<i64> = Vec::new();
        let scan_start = std::time::Instant::now();
        while record_timestamps.len() < 3 && scan_start.elapsed() < Duration::from_secs(10) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll records");
            for rec in scan_records {
                record_timestamps.push(rec.timestamp());
            }
        }
        assert_eq!(record_timestamps.len(), 3, "Expected 3 record timestamps");

        let min_ts = *record_timestamps.iter().min().unwrap();
        let max_ts = *record_timestamps.iter().max().unwrap();

        // Timestamp before all records should resolve to offset 0
        let before_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(min_ts - 1))
            .await
            .expect("Failed to list offsets by timestamp (before)");

        assert_eq!(
            before_offsets.get(&0),
            Some(&0),
            "Timestamp before first record should resolve to offset 0"
        );

        // Timestamp after all records should resolve to offset 3
        let after_offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Timestamp(max_ts + 1))
            .await
            .expect("Failed to list offsets by timestamp (after)");

        assert_eq!(
            after_offsets.get(&0),
            Some(&3),
            "Timestamp after last record should resolve to offset 3"
        );
    }

    #[tokio::test]
    async fn test_project() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_project");

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
            .create_writer()
            .expect("Failed to create writer");

        let batch = record_batch!(
            ("col_a", Int32, [1, 2, 3]),
            ("col_b", Utf8, ["x", "y", "z"]),
            ("col_c", Int32, [10, 20, 30])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(batch)
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
                row.get_string(0).unwrap(),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1).unwrap(),
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
                row.get_string(0).unwrap(),
                expected_col_b[i],
                "col_b mismatch at index {}",
                i
            );
            assert_eq!(
                row.get_int(1).unwrap(),
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
            .subscribe_buckets(&bucket_offsets)
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
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_poll_batches");
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

        let writer = table.new_append().unwrap().create_writer().unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [1, 2]), ("name", Utf8, ["a", "b"])).unwrap(),
            )
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [3, 4]), ("name", Utf8, ["c", "d"])).unwrap(),
            )
            .unwrap();
        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [5, 6]), ("name", Utf8, ["e", "f"])).unwrap(),
            )
            .unwrap();
        writer.flush().await.unwrap();

        use arrow::array::Int32Array;

        fn extract_ids(batches: &[fluss::record::ScanBatch]) -> Vec<i32> {
            batches
                .iter()
                .flat_map(|b| {
                    let batch = b.batch();
                    (0..batch.num_rows()).map(move |i| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(i)
                    })
                })
                .collect()
        }

        // poll may return partial results if not all batches are available yet,
        // so we accumulate across multiple polls until we have the expected count.
        let mut all_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while all_ids.len() < 6 && tokio::time::Instant::now() < deadline {
            let batches = scanner.poll(Duration::from_secs(5)).await.unwrap();
            all_ids.extend(extract_ids(&batches));
        }

        // Test 2: Order should be preserved across multiple batches
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5, 6]);

        writer
            .append_arrow_batch(
                record_batch!(("id", Int32, [7, 8]), ("name", Utf8, ["g", "h"])).unwrap(),
            )
            .unwrap();
        writer.flush().await.unwrap();

        let mut new_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while new_ids.len() < 2 && tokio::time::Instant::now() < deadline {
            let more = scanner.poll(Duration::from_secs(5)).await.unwrap();
            new_ids.extend(extract_ids(&more));
        }

        // Test 3: Subsequent polls should not return duplicate data (offset continuation)
        assert_eq!(new_ids, vec![7, 8]);

        // Test 4: Subscribing from mid-offset should truncate batch (Arrow batch slicing)
        // Server returns all records from start of batch, but client truncates to subscription offset
        let trunc_scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
        trunc_scanner.subscribe(0, 3).await.unwrap();
        let mut trunc_ids = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while trunc_ids.len() < 5 && tokio::time::Instant::now() < deadline {
            let trunc_batches = trunc_scanner.poll(Duration::from_secs(5)).await.unwrap();
            trunc_ids.extend(extract_ids(&trunc_batches));
        }

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
        assert_eq!(proj_batches[0].batch().num_columns(), 1);
    }

    /// Integration test covering produce and scan operations for all supported datatypes
    /// in log tables.
    #[tokio::test]
    async fn all_supported_datatypes() {
        use fluss::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_all_datatypes");

        // Create a log table with all supported datatypes for append/scan
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    // Integer types
                    .column("col_tinyint", DataTypes::tinyint())
                    .column("col_smallint", DataTypes::smallint())
                    .column("col_int", DataTypes::int())
                    .column("col_bigint", DataTypes::bigint())
                    // Floating point types
                    .column("col_float", DataTypes::float())
                    .column("col_double", DataTypes::double())
                    // Boolean type
                    .column("col_boolean", DataTypes::boolean())
                    // Char type
                    .column("col_char", DataTypes::char(10))
                    // String type
                    .column("col_string", DataTypes::string())
                    // Decimal type
                    .column("col_decimal", DataTypes::decimal(10, 2))
                    // Date type
                    .column("col_date", DataTypes::date())
                    // Time types
                    .column("col_time_s", DataTypes::time_with_precision(0))
                    .column("col_time_ms", DataTypes::time_with_precision(3))
                    .column("col_time_us", DataTypes::time_with_precision(6))
                    .column("col_time_ns", DataTypes::time_with_precision(9))
                    // Timestamp types
                    .column("col_timestamp_s", DataTypes::timestamp_with_precision(0))
                    .column("col_timestamp_ms", DataTypes::timestamp_with_precision(3))
                    .column("col_timestamp_us", DataTypes::timestamp_with_precision(6))
                    .column("col_timestamp_ns", DataTypes::timestamp_with_precision(9))
                    // Timestamp_ltz types
                    .column(
                        "col_timestamp_ltz_s",
                        DataTypes::timestamp_ltz_with_precision(0),
                    )
                    .column(
                        "col_timestamp_ltz_ms",
                        DataTypes::timestamp_ltz_with_precision(3),
                    )
                    .column(
                        "col_timestamp_ltz_us",
                        DataTypes::timestamp_ltz_with_precision(6),
                    )
                    .column(
                        "col_timestamp_ltz_ns",
                        DataTypes::timestamp_ltz_with_precision(9),
                    )
                    // Bytes type
                    .column("col_bytes", DataTypes::bytes())
                    // Fixed-size binary type
                    .column("col_binary", DataTypes::binary(4))
                    // Timestamp types with negative values (before Unix epoch)
                    .column(
                        "col_timestamp_us_neg",
                        DataTypes::timestamp_with_precision(6),
                    )
                    .column(
                        "col_timestamp_ns_neg",
                        DataTypes::timestamp_with_precision(9),
                    )
                    .column(
                        "col_timestamp_ltz_us_neg",
                        DataTypes::timestamp_ltz_with_precision(6),
                    )
                    .column(
                        "col_timestamp_ltz_ns_neg",
                        DataTypes::timestamp_ltz_with_precision(9),
                    )
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

        let field_count = table.get_table_info().schema.columns().len();

        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Test data for all datatypes
        let col_tinyint = 127i8;
        let col_smallint = 32767i16;
        let col_int = 2147483647i32;
        let col_bigint = 9223372036854775807i64;
        let col_float = 3.14f32;
        let col_double = 2.718281828459045f64;
        let col_boolean = true;
        let col_char = "hello";
        let col_string = "world of fluss rust client";
        let col_decimal = Decimal::from_unscaled_long(12345, 10, 2).unwrap(); // 123.45
        let col_date = Date::new(20476); // 2026-01-23
        let col_time_s = Time::new(36827000); // 10:13:47
        let col_time_ms = Time::new(36827123); // 10:13:47.123
        let col_time_us = Time::new(86399999); // 23:59:59.999
        let col_time_ns = Time::new(1); // 00:00:00.001
        // 2026-01-23 10:13:47 UTC
        let col_timestamp_s = TimestampNtz::new(1769163227000);
        // 2026-01-23 10:13:47.123 UTC
        let col_timestamp_ms = TimestampNtz::new(1769163227123);
        // 2026-01-23 10:13:47.123456 UTC
        let col_timestamp_us = TimestampNtz::from_millis_nanos(1769163227123, 456000).unwrap();
        // 2026-01-23 10:13:47.123999999 UTC
        let col_timestamp_ns = TimestampNtz::from_millis_nanos(1769163227123, 999_999).unwrap();
        let col_timestamp_ltz_s = TimestampLtz::new(1769163227000);
        let col_timestamp_ltz_ms = TimestampLtz::new(1769163227123);
        let col_timestamp_ltz_us = TimestampLtz::from_millis_nanos(1769163227123, 456000).unwrap();
        let col_timestamp_ltz_ns = TimestampLtz::from_millis_nanos(1769163227123, 999_999).unwrap();
        let col_bytes: Vec<u8> = b"binary data".to_vec();
        let col_binary: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

        // 1960-06-15 08:30:45.123456 UTC (before 1970)
        let col_timestamp_us_neg = TimestampNtz::from_millis_nanos(-301234154877, 456000).unwrap();
        // 1960-06-15 08:30:45.123999999 UTC (before 1970)
        let col_timestamp_ns_neg = TimestampNtz::from_millis_nanos(-301234154877, 999_999).unwrap();
        let col_timestamp_ltz_us_neg =
            TimestampLtz::from_millis_nanos(-301234154877, 456000).unwrap();
        let col_timestamp_ltz_ns_neg =
            TimestampLtz::from_millis_nanos(-301234154877, 999_999).unwrap();

        // Append a row with all datatypes
        let mut row = GenericRow::new(field_count);
        row.set_field(0, col_tinyint);
        row.set_field(1, col_smallint);
        row.set_field(2, col_int);
        row.set_field(3, col_bigint);
        row.set_field(4, col_float);
        row.set_field(5, col_double);
        row.set_field(6, col_boolean);
        row.set_field(7, col_char);
        row.set_field(8, col_string);
        row.set_field(9, col_decimal.clone());
        row.set_field(10, col_date);
        row.set_field(11, col_time_s);
        row.set_field(12, col_time_ms);
        row.set_field(13, col_time_us);
        row.set_field(14, col_time_ns);
        row.set_field(15, col_timestamp_s);
        row.set_field(16, col_timestamp_ms);
        row.set_field(17, col_timestamp_us.clone());
        row.set_field(18, col_timestamp_ns.clone());
        row.set_field(19, col_timestamp_ltz_s);
        row.set_field(20, col_timestamp_ltz_ms);
        row.set_field(21, col_timestamp_ltz_us.clone());
        row.set_field(22, col_timestamp_ltz_ns.clone());
        row.set_field(23, col_bytes.as_slice());
        row.set_field(24, col_binary.as_slice());
        row.set_field(25, col_timestamp_us_neg.clone());
        row.set_field(26, col_timestamp_ns_neg.clone());
        row.set_field(27, col_timestamp_ltz_us_neg.clone());
        row.set_field(28, col_timestamp_ltz_ns_neg.clone());

        append_writer
            .append(&row)
            .expect("Failed to append row with all datatypes");

        // Append a row with null values for all columns
        let mut row_with_nulls = GenericRow::new(field_count);
        for i in 0..field_count {
            row_with_nulls.set_field(i, Datum::Null);
        }

        append_writer
            .append(&row_with_nulls)
            .expect("Failed to append row with nulls");

        append_writer.flush().await.expect("Failed to flush");

        // Scan the records
        let records = scan_table(&table, |scan| scan).await;

        assert_eq!(records.len(), 2, "Expected 2 records");

        let found_row = records[0].row();
        assert_eq!(
            found_row.get_byte(0).unwrap(),
            col_tinyint,
            "col_tinyint mismatch"
        );
        assert_eq!(
            found_row.get_short(1).unwrap(),
            col_smallint,
            "col_smallint mismatch"
        );
        assert_eq!(found_row.get_int(2).unwrap(), col_int, "col_int mismatch");
        assert_eq!(
            found_row.get_long(3).unwrap(),
            col_bigint,
            "col_bigint mismatch"
        );
        assert!(
            (found_row.get_float(4).unwrap() - col_float).abs() < f32::EPSILON,
            "col_float mismatch: expected {}, got {}",
            col_float,
            found_row.get_float(4).unwrap()
        );
        assert!(
            (found_row.get_double(5).unwrap() - col_double).abs() < f64::EPSILON,
            "col_double mismatch: expected {}, got {}",
            col_double,
            found_row.get_double(5).unwrap()
        );
        assert_eq!(
            found_row.get_boolean(6).unwrap(),
            col_boolean,
            "col_boolean mismatch"
        );
        assert_eq!(
            found_row.get_char(7, 10).unwrap(),
            col_char,
            "col_char mismatch"
        );
        assert_eq!(
            found_row.get_string(8).unwrap(),
            col_string,
            "col_string mismatch"
        );
        assert_eq!(
            found_row.get_decimal(9, 10, 2).unwrap(),
            col_decimal,
            "col_decimal mismatch"
        );
        assert_eq!(
            found_row.get_date(10).unwrap().get_inner(),
            col_date.get_inner(),
            "col_date mismatch"
        );

        assert_eq!(
            found_row.get_time(11).unwrap().get_inner(),
            col_time_s.get_inner(),
            "col_time_s mismatch"
        );

        assert_eq!(
            found_row.get_time(12).unwrap().get_inner(),
            col_time_ms.get_inner(),
            "col_time_ms mismatch"
        );

        assert_eq!(
            found_row.get_time(13).unwrap().get_inner(),
            col_time_us.get_inner(),
            "col_time_us mismatch"
        );

        assert_eq!(
            found_row.get_time(14).unwrap().get_inner(),
            col_time_ns.get_inner(),
            "col_time_ns mismatch"
        );

        assert_eq!(
            found_row
                .get_timestamp_ntz(15, 0)
                .unwrap()
                .get_millisecond(),
            col_timestamp_s.get_millisecond(),
            "col_timestamp_s mismatch"
        );

        assert_eq!(
            found_row
                .get_timestamp_ntz(16, 3)
                .unwrap()
                .get_millisecond(),
            col_timestamp_ms.get_millisecond(),
            "col_timestamp_ms mismatch"
        );

        let read_ts_us = found_row.get_timestamp_ntz(17, 6).unwrap();
        assert_eq!(
            read_ts_us.get_millisecond(),
            col_timestamp_us.get_millisecond(),
            "col_timestamp_us millis mismatch"
        );
        assert_eq!(
            read_ts_us.get_nano_of_millisecond(),
            col_timestamp_us.get_nano_of_millisecond(),
            "col_timestamp_us nanos mismatch"
        );

        let read_ts_ns = found_row.get_timestamp_ntz(18, 9).unwrap();
        assert_eq!(
            read_ts_ns.get_millisecond(),
            col_timestamp_ns.get_millisecond(),
            "col_timestamp_ns millis mismatch"
        );
        assert_eq!(
            read_ts_ns.get_nano_of_millisecond(),
            col_timestamp_ns.get_nano_of_millisecond(),
            "col_timestamp_ns nanos mismatch"
        );

        assert_eq!(
            found_row
                .get_timestamp_ltz(19, 0)
                .unwrap()
                .get_epoch_millisecond(),
            col_timestamp_ltz_s.get_epoch_millisecond(),
            "col_timestamp_ltz_s mismatch"
        );

        assert_eq!(
            found_row
                .get_timestamp_ltz(20, 3)
                .unwrap()
                .get_epoch_millisecond(),
            col_timestamp_ltz_ms.get_epoch_millisecond(),
            "col_timestamp_ltz_ms mismatch"
        );

        let read_ts_ltz_us = found_row.get_timestamp_ltz(21, 6).unwrap();
        assert_eq!(
            read_ts_ltz_us.get_epoch_millisecond(),
            col_timestamp_ltz_us.get_epoch_millisecond(),
            "col_timestamp_ltz_us millis mismatch"
        );
        assert_eq!(
            read_ts_ltz_us.get_nano_of_millisecond(),
            col_timestamp_ltz_us.get_nano_of_millisecond(),
            "col_timestamp_ltz_us nanos mismatch"
        );

        let read_ts_ltz_ns = found_row.get_timestamp_ltz(22, 9).unwrap();
        assert_eq!(
            read_ts_ltz_ns.get_epoch_millisecond(),
            col_timestamp_ltz_ns.get_epoch_millisecond(),
            "col_timestamp_ltz_ns millis mismatch"
        );
        assert_eq!(
            read_ts_ltz_ns.get_nano_of_millisecond(),
            col_timestamp_ltz_ns.get_nano_of_millisecond(),
            "col_timestamp_ltz_ns nanos mismatch"
        );
        assert_eq!(
            found_row.get_bytes(23).unwrap(),
            col_bytes,
            "col_bytes mismatch"
        );
        assert_eq!(
            found_row.get_binary(24, 4).unwrap(),
            col_binary,
            "col_binary mismatch"
        );

        // Verify timestamps before Unix epoch (negative timestamps)
        let read_ts_us_neg = found_row.get_timestamp_ntz(25, 6).unwrap();
        assert_eq!(
            read_ts_us_neg.get_millisecond(),
            col_timestamp_us_neg.get_millisecond(),
            "col_timestamp_us_neg millis mismatch"
        );
        assert_eq!(
            read_ts_us_neg.get_nano_of_millisecond(),
            col_timestamp_us_neg.get_nano_of_millisecond(),
            "col_timestamp_us_neg nanos mismatch"
        );

        let read_ts_ns_neg = found_row.get_timestamp_ntz(26, 9).unwrap();
        assert_eq!(
            read_ts_ns_neg.get_millisecond(),
            col_timestamp_ns_neg.get_millisecond(),
            "col_timestamp_ns_neg millis mismatch"
        );
        assert_eq!(
            read_ts_ns_neg.get_nano_of_millisecond(),
            col_timestamp_ns_neg.get_nano_of_millisecond(),
            "col_timestamp_ns_neg nanos mismatch"
        );

        let read_ts_ltz_us_neg = found_row.get_timestamp_ltz(27, 6).unwrap();
        assert_eq!(
            read_ts_ltz_us_neg.get_epoch_millisecond(),
            col_timestamp_ltz_us_neg.get_epoch_millisecond(),
            "col_timestamp_ltz_us_neg millis mismatch"
        );
        assert_eq!(
            read_ts_ltz_us_neg.get_nano_of_millisecond(),
            col_timestamp_ltz_us_neg.get_nano_of_millisecond(),
            "col_timestamp_ltz_us_neg nanos mismatch"
        );

        let read_ts_ltz_ns_neg = found_row.get_timestamp_ltz(28, 9).unwrap();
        assert_eq!(
            read_ts_ltz_ns_neg.get_epoch_millisecond(),
            col_timestamp_ltz_ns_neg.get_epoch_millisecond(),
            "col_timestamp_ltz_ns_neg millis mismatch"
        );
        assert_eq!(
            read_ts_ltz_ns_neg.get_nano_of_millisecond(),
            col_timestamp_ltz_ns_neg.get_nano_of_millisecond(),
            "col_timestamp_ltz_ns_neg nanos mismatch"
        );

        // Verify row with all nulls (record index 1)
        let found_row_nulls = records[1].row();
        for i in 0..field_count {
            assert!(
                found_row_nulls.is_null_at(i).unwrap(),
                "column {} should be null",
                i
            );
        }

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn partitioned_table_append_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_partitioned_log_append");

        // Create a partitioned log table
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("region", DataTypes::string())
                    .column("value", DataTypes::bigint())
                    .build()
                    .expect("Failed to build schema"),
            )
            .partitioned_by(vec!["region"])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        // Create partitions
        create_partitions(&admin, &table_path, "region", &["US", "EU"]).await;

        // Wait for partitions to be available
        tokio::time::sleep(Duration::from_secs(2)).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        // Create append writer - this should now work for partitioned tables
        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Append records with different partitions
        let test_data = [
            (1, "US", 100i64),
            (2, "US", 200i64),
            (3, "EU", 300i64),
            (4, "EU", 400i64),
        ];

        for (id, region, value) in &test_data {
            let mut row = fluss::row::GenericRow::new(3);
            row.set_field(0, *id);
            row.set_field(1, *region);
            row.set_field(2, *value);
            append_writer.append(&row).expect("Failed to append row");
        }

        append_writer.flush().await.expect("Failed to flush");

        // Test append_arrow_batch for partitioned tables
        // Each batch must contain rows from the same partition
        let us_batch = record_batch!(
            ("id", Int32, [5, 6]),
            ("region", Utf8, ["US", "US"]),
            ("value", Int64, [500, 600])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(us_batch)
            .expect("Failed to append US batch");

        let eu_batch = record_batch!(
            ("id", Int32, [7, 8]),
            ("region", Utf8, ["EU", "EU"]),
            ("value", Int64, [700, 800])
        )
        .unwrap();
        append_writer
            .append_arrow_batch(eu_batch)
            .expect("Failed to append EU batch");

        append_writer
            .flush()
            .await
            .expect("Failed to flush batches");

        // Test list_offsets_for_partition
        // US partition has 4 records: 2 from row append + 2 from batch append
        let us_offsets = admin
            .list_partition_offsets(&table_path, "US", &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list offsets for US partition");
        assert_eq!(
            us_offsets.get(&0),
            Some(&4),
            "US partition should have 4 records"
        );

        // EU partition has 4 records: 2 from row append + 2 from batch append
        let eu_offsets = admin
            .list_partition_offsets(&table_path, "EU", &[0], OffsetSpec::Latest)
            .await
            .expect("Failed to list offsets for EU partition");
        assert_eq!(
            eu_offsets.get(&0),
            Some(&4),
            "EU partition should have 4 records"
        );

        // test list a not exist partition should return error
        let result = admin
            .list_partition_offsets(&table_path, "NOT Exists", &[0], OffsetSpec::Latest)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "Table partition 'fluss.test_partitioned_log_append(p=NOT Exists)' does not exist."
        ));

        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner");
        let partition_info = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        for partition_info in partition_info {
            log_scanner
                .subscribe_partition(partition_info.get_partition_id(), 0, 0)
                .await
                .expect("Failed to subscribe to partition");
        }

        let expected_records = vec![
            (1, "US", 100i64),
            (2, "US", 200i64),
            (3, "EU", 300i64),
            (4, "EU", 400),
            (5, "US", 500i64),
            (6, "US", 600i64),
            (7, "EU", 700i64),
            (8, "EU", 800i64),
        ];
        let expected_records: Vec<(i32, String, i64)> = expected_records
            .into_iter()
            .map(|(id, region, val)| (id, region.to_string(), val))
            .collect();

        let mut collected_records: Vec<(i32, String, i64)> = Vec::new();
        let start_time = std::time::Instant::now();
        while collected_records.len() < expected_records.len()
            && start_time.elapsed() < Duration::from_secs(10)
        {
            let records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll log scanner");
            for rec in records {
                let row = rec.row();
                collected_records.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }

        assert_eq!(
            collected_records.len(),
            expected_records.len(),
            "Did not receive all records in time, expect receive {} records, but got {} records",
            expected_records.len(),
            collected_records.len()
        );
        collected_records.sort_by_key(|r| r.0);
        assert_eq!(
            collected_records, expected_records,
            "Data mismatch between sent and received"
        );

        // Test unsubscribe_partition: after unsubscribing from one partition,
        // data from that partition should no longer be read.
        let log_scanner_unsub = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner for unsubscribe test");
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let eu_partition_id = partition_infos
            .iter()
            .find(|p| p.get_partition_name() == "EU")
            .map(|p| p.get_partition_id())
            .expect("EU partition should exist");
        for info in &partition_infos {
            log_scanner_unsub
                .subscribe_partition(info.get_partition_id(), 0, 0)
                .await
                .expect("Failed to subscribe to partition");
        }
        log_scanner_unsub
            .unsubscribe_partition(eu_partition_id, 0)
            .await
            .expect("Failed to unsubscribe from EU partition");

        let mut records_after_unsubscribe: Vec<(i32, String, i64)> = Vec::new();
        let unsub_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while records_after_unsubscribe.len() < 4 && std::time::Instant::now() < unsub_deadline {
            let records = log_scanner_unsub
                .poll(Duration::from_millis(300))
                .await
                .expect("Failed to poll after unsubscribe");
            for rec in records {
                let row = rec.row();
                records_after_unsubscribe.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }

        assert!(
            records_after_unsubscribe.iter().all(|r| r.1 == "US"),
            "After unsubscribe_partition(EU), only US partition data should be read; got regions: {:?}",
            records_after_unsubscribe
                .iter()
                .map(|r| r.1.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            records_after_unsubscribe.len(),
            4,
            "Should receive exactly 4 US records (ids 1,2,5,6); got {}",
            records_after_unsubscribe.len()
        );

        // Test subscribe_partition_buckets: batch subscribe to all partitions at once
        let log_scanner_batch = table
            .new_scan()
            .create_log_scanner()
            .expect("Failed to create log scanner for batch partition subscribe test");
        let partition_infos = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partition infos");
        let partition_bucket_offsets: HashMap<(i64, i32), i64> = partition_infos
            .iter()
            .map(|p| ((p.get_partition_id(), 0), 0i64))
            .collect();
        log_scanner_batch
            .subscribe_partition_buckets(&partition_bucket_offsets)
            .await
            .expect("Failed to batch subscribe to partitions");

        let mut batch_collected: Vec<(i32, String, i64)> = Vec::new();
        let batch_start = std::time::Instant::now();
        while batch_collected.len() < expected_records.len()
            && batch_start.elapsed() < Duration::from_secs(10)
        {
            let records = log_scanner_batch
                .poll(Duration::from_millis(500))
                .await
                .expect("Failed to poll after batch partition subscribe");
            for rec in records {
                let row = rec.row();
                batch_collected.push((
                    row.get_int(0).unwrap(),
                    row.get_string(1).unwrap().to_string(),
                    row.get_long(2).unwrap(),
                ));
            }
        }
        assert_eq!(
            batch_collected.len(),
            expected_records.len(),
            "Did not receive all records in time, expect receive {} records, but got {} records",
            expected_records.len(),
            batch_collected.len()
        );
        batch_collected.sort_by_key(|r| r.0);
        assert_eq!(
            batch_collected, expected_records,
            "subscribe_partition_buckets should receive the same records as subscribe_partition loop"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn undersized_row_returns_error() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_undersized_row");

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("col_bool", DataTypes::boolean())
                    .column("col_int", DataTypes::int())
                    .column("col_string", DataTypes::string())
                    .column("col_bigint", DataTypes::bigint())
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
            .expect("Failed to create table append")
            .create_writer()
            .expect("Failed to create writer");

        // Scenario 1b: GenericRow with only 2 fields for a 4-column table
        let mut row = fluss::row::GenericRow::new(2);
        row.set_field(0, true);
        row.set_field(1, 42_i32);

        let result = append_writer.append(&row);
        assert!(result.is_err(), "Undersized row should be rejected");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Expected: 4") && err_msg.contains("Actual: 2"),
            "Error should mention field count mismatch, got: {err_msg}"
        );

        // Correct column count but wrong types:
        // Schema is (Boolean, Int, String, BigInt) but we put Int64 where String is expected.
        // This should return an error, not panic.
        let row_wrong_types = fluss::row::GenericRow::from_data(vec![
            fluss::row::Datum::Bool(true),
            fluss::row::Datum::Int32(42),
            fluss::row::Datum::Int64(999), // wrong: String column
            fluss::row::Datum::Int64(100),
        ]);

        let result = append_writer.append(&row_wrong_types);
        assert!(
            result.is_err(),
            "Row with mismatched types should be rejected, not panic"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
