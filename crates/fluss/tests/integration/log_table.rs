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
    use crate::integration::utils::{
        create_partitions, create_table, get_shared_cluster, make_int_array, make_string_array,
    };
    use arrow::array::record_batch;
    use fluss::client::{EARLIEST_OFFSET, FlussTable, TableScan};
    use fluss::metadata::{DataField, DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::ScanRecord;
    use fluss::row::binary_array::FlussArrayWriter;
    use fluss::row::{
        Date, Datum, Decimal, FlussArray, GenericRow, InternalRow, Time, TimestampLtz, TimestampNtz,
    };
    use fluss::rpc::message::OffsetSpec;
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn append_record_batch_and_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

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

        let admin = connection.get_admin().expect("Failed to get admin");

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

        let admin = connection.get_admin().expect("Failed to get admin");

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
        let admin = connection.get_admin().expect("Failed to get admin");

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
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

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
                    .column("col_array", DataTypes::array(DataTypes::string()))
                    .column(
                        "col_row",
                        DataTypes::row(vec![
                            DataField::new("seq", DataTypes::int(), None),
                            DataField::new("label", DataTypes::string(), None),
                        ]),
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
        let col_float = std::f32::consts::PI;
        let col_double = std::f64::consts::E;
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

        let col_array = make_string_array(&[Some("fluss"), Some("rust")]);

        let mut col_row_inner = GenericRow::new(2);
        col_row_inner.set_field(0, 7_i32);
        col_row_inner.set_field(1, "lumiere");

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
        row.set_field(17, col_timestamp_us);
        row.set_field(18, col_timestamp_ns);
        row.set_field(19, col_timestamp_ltz_s);
        row.set_field(20, col_timestamp_ltz_ms);
        row.set_field(21, col_timestamp_ltz_us);
        row.set_field(22, col_timestamp_ltz_ns);
        row.set_field(23, col_bytes.as_slice());
        row.set_field(24, col_binary.as_slice());
        row.set_field(25, col_timestamp_us_neg);
        row.set_field(26, col_timestamp_ns_neg);
        row.set_field(27, col_timestamp_ltz_us_neg);
        row.set_field(28, col_timestamp_ltz_ns_neg);
        row.set_field(29, col_array);
        row.set_field(30, Datum::Row(Box::new(col_row_inner)));

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

        let arr = found_row.get_array(29).unwrap();
        assert_eq!(arr.size(), 2, "col_array size mismatch");
        assert_eq!(arr.get_string(0).unwrap(), "fluss", "col_array[0] mismatch");
        assert_eq!(arr.get_string(1).unwrap(), "rust", "col_array[1] mismatch");

        let nested = found_row.get_row(30).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 7, "col_row.seq mismatch");
        assert_eq!(
            nested.get_string(1).unwrap(),
            "lumiere",
            "col_row.label mismatch"
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
    async fn test_map_datatype_roundtrip() {
        use fluss::row::binary_map::FlussMapWriter;
        use fluss::row::{Datum, GenericRow};

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_map_datatype_roundtrip");

        let key_type = DataTypes::string();
        let value_type = DataTypes::int();
        let map_type = DataTypes::map(key_type.clone(), value_type.clone());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("map_col", map_type.clone())
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

        // 1. Construct FlussMap
        let mut map_writer = FlussMapWriter::new(3, &key_type, &value_type);
        map_writer.write_entry("k1".into(), 10.into()).unwrap();
        map_writer.write_entry("k2".into(), 20.into()).unwrap();
        map_writer.write_entry("k3".into(), 30.into()).unwrap();
        let fluss_map = map_writer.complete().unwrap();

        // 2. Insert Row
        let mut row = GenericRow::new(2);
        row.set_field(0, 1i32);
        row.set_field(1, Datum::Map(fluss_map));

        let append_writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        append_writer.append(&row).expect("Failed to append row");
        append_writer.flush().await.expect("Failed to flush");

        // 3. Fetch Row
        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 1, "Expected 1 record");

        let found_row = records[0].row();
        assert_eq!(found_row.get_int(0).unwrap(), 1);

        // 4. Assert Map
        let decoded_map = found_row
            .get_map(1, &key_type, &value_type)
            .expect("Failed to get map");
        assert_eq!(decoded_map.size(), 3);

        let decoded_keys = decoded_map.key_array();
        let decoded_values = decoded_map.value_array();

        assert_eq!(decoded_keys.get_string(0).unwrap(), "k1");
        assert_eq!(decoded_keys.get_string(1).unwrap(), "k2");
        assert_eq!(decoded_keys.get_string(2).unwrap(), "k3");

        assert_eq!(decoded_values.get_int(0).unwrap(), 10);
        assert_eq!(decoded_values.get_int(1).unwrap(), 20);
        assert_eq!(decoded_values.get_int(2).unwrap(), 30);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn partitioned_table_append_scan() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().expect("Failed to get admin");

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
        let admin = connection.get_admin().expect("Failed to get admin");

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

    #[tokio::test]
    async fn append_and_scan_with_array() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_arrays");
        let inner_array_type = DataTypes::array(DataTypes::int());

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("tags", DataTypes::array(DataTypes::string()))
            .column("scores", DataTypes::array(DataTypes::int()))
            .column("matrix", DataTypes::array(inner_array_type.clone()))
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut row1 = GenericRow::new(4);
        row1.set_field(0, 1_i32);
        row1.set_field(1, make_string_array(&[Some("hello"), Some("world")]));
        row1.set_field(2, make_int_array(&[Some(10), Some(20), Some(30)]));
        let m1 = {
            let mut w = FlussArrayWriter::new(2, &inner_array_type);
            w.write_array(0, &make_int_array(&[Some(1), Some(2)]));
            w.write_array(1, &make_int_array(&[Some(3), Some(4)]));
            w.complete().expect("matrix1")
        };
        row1.set_field(3, m1);

        let mut row2 = GenericRow::new(4);
        row2.set_field(0, 2_i32);
        row2.set_field(1, make_string_array(&[None]));
        row2.set_field(2, make_int_array(&[]));
        let m2 = {
            let mut w = FlussArrayWriter::new(3, &inner_array_type);
            w.write_array(0, &make_int_array(&[Some(5)]));
            w.set_null_at(1);
            w.write_array(2, &make_int_array(&[]));
            w.complete().expect("matrix2")
        };
        row2.set_field(3, m2);

        let mut row3 = GenericRow::new(4);
        row3.set_field(0, 3_i32);
        row3.set_field(1, Datum::Null);
        row3.set_field(2, make_int_array(&[Some(42)]));
        row3.set_field(3, Datum::Null);

        append_writer.append(&row1).expect("append row1");
        append_writer.append(&row2).expect("append row2");
        append_writer.append(&row3).expect("append row3");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 3, "expected three log records");

        let r0 = records[0].row();
        assert_eq!(r0.get_int(0).unwrap(), 1);
        let tags_r0 = r0.get_array(1).unwrap();
        assert_eq!(tags_r0.size(), 2);
        assert_eq!(tags_r0.get_string(0).unwrap(), "hello");
        assert_eq!(tags_r0.get_string(1).unwrap(), "world");
        let scores_r0 = r0.get_array(2).unwrap();
        assert_eq!(scores_r0.size(), 3);
        assert_eq!(scores_r0.get_int(0).unwrap(), 10);
        assert_eq!(scores_r0.get_int(1).unwrap(), 20);
        assert_eq!(scores_r0.get_int(2).unwrap(), 30);
        let matrix_r0: FlussArray = r0.get_array(3).unwrap();
        assert_eq!(matrix_r0.size(), 2);
        let mr0_0 = matrix_r0.get_array(0).unwrap();
        assert_eq!(mr0_0.size(), 2);
        assert_eq!(mr0_0.get_int(0).unwrap(), 1);
        assert_eq!(mr0_0.get_int(1).unwrap(), 2);
        let mr0_1 = matrix_r0.get_array(1).unwrap();
        assert_eq!(mr0_1.size(), 2);
        assert_eq!(mr0_1.get_int(0).unwrap(), 3);
        assert_eq!(mr0_1.get_int(1).unwrap(), 4);

        let r1 = records[1].row();
        assert_eq!(r1.get_int(0).unwrap(), 2);
        let tags_r1 = r1.get_array(1).unwrap();
        assert_eq!(tags_r1.size(), 1);
        assert!(tags_r1.is_null_at(0));
        let scores_r1 = r1.get_array(2).unwrap();
        assert_eq!(scores_r1.size(), 0);
        let matrix_r1 = r1.get_array(3).unwrap();
        assert_eq!(matrix_r1.size(), 3);
        let mr1_0 = matrix_r1.get_array(0).unwrap();
        assert_eq!(mr1_0.size(), 1);
        assert_eq!(mr1_0.get_int(0).unwrap(), 5);
        assert!(matrix_r1.is_null_at(1));
        let mr1_2 = matrix_r1.get_array(2).unwrap();
        assert_eq!(mr1_2.size(), 0);

        let r2 = records[2].row();
        assert_eq!(r2.get_int(0).unwrap(), 3);
        assert!(r2.is_null_at(1).unwrap());
        let scores_r2 = r2.get_array(2).unwrap();
        assert_eq!(scores_r2.size(), 1);
        assert_eq!(scores_r2.get_int(0).unwrap(), 42);
        assert!(r2.is_null_at(3).unwrap());

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn append_and_scan_with_array_of_row() {
        use fluss::metadata::{DataField, DataType};

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_array_of_row");

        let event_row_type_owned = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let array_of_row_type = DataTypes::array(event_row_type_owned.clone());

        let event_row_type = match &event_row_type_owned {
            DataType::Row(rt) => rt.clone(),
            _ => unreachable!(),
        };

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("events", array_of_row_type)
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut events1 = FlussArrayWriter::new(2, &event_row_type_owned);
        let mut e0 = GenericRow::new(2);
        e0.set_field(0, 1_i32);
        e0.set_field(1, "open");
        events1.write_row(0, &e0).expect("write e0");
        let mut e1 = GenericRow::new(2);
        e1.set_field(0, 2_i32);
        e1.set_field(1, "close");
        events1.write_row(1, &e1).expect("write e1");
        let events1 = events1.complete().expect("events1");

        let mut row1 = GenericRow::new(2);
        row1.set_field(0, 1_i32);
        row1.set_field(1, events1);

        let mut events2 = FlussArrayWriter::new(3, &event_row_type_owned);
        let mut e2 = GenericRow::new(2);
        e2.set_field(0, 7_i32);
        e2.set_field(1, "x");
        events2.write_row(0, &e2).expect("write e2");
        events2.set_null_at(1);
        let mut e3 = GenericRow::new(2);
        e3.set_field(0, 8_i32);
        e3.set_field(1, "y");
        events2.write_row(2, &e3).expect("write e3");
        let events2 = events2.complete().expect("events2");

        let mut row2 = GenericRow::new(2);
        row2.set_field(0, 2_i32);
        row2.set_field(1, events2);

        let mut row3 = GenericRow::new(2);
        row3.set_field(0, 3_i32);
        row3.set_field(1, Datum::Null);

        append_writer.append(&row1).expect("append row1");
        append_writer.append(&row2).expect("append row2");
        append_writer.append(&row3).expect("append row3");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 3, "expected three log records");

        let r0 = records[0].row();
        assert_eq!(r0.get_int(0).unwrap(), 1);
        let events_r0 = r0.get_array(1).unwrap();
        assert_eq!(events_r0.size(), 2);
        let e0_r0 = events_r0.get_row(0, &event_row_type).unwrap();
        assert_eq!(e0_r0.get_int(0).unwrap(), 1);
        assert_eq!(e0_r0.get_string(1).unwrap(), "open");
        let e1_r0 = events_r0.get_row(1, &event_row_type).unwrap();
        assert_eq!(e1_r0.get_int(0).unwrap(), 2);
        assert_eq!(e1_r0.get_string(1).unwrap(), "close");

        let r1 = records[1].row();
        let events_r1 = r1.get_array(1).unwrap();
        assert_eq!(events_r1.size(), 3);
        let e0_r1 = events_r1.get_row(0, &event_row_type).unwrap();
        assert_eq!(e0_r1.get_int(0).unwrap(), 7);
        assert_eq!(e0_r1.get_string(1).unwrap(), "x");
        assert!(events_r1.is_null_at(1));
        let e2_r1 = events_r1.get_row(2, &event_row_type).unwrap();
        assert_eq!(e2_r1.get_int(0).unwrap(), 8);
        assert_eq!(e2_r1.get_string(1).unwrap(), "y");

        let r2 = records[2].row();
        assert_eq!(r2.get_int(0).unwrap(), 3);
        assert!(r2.is_null_at(1).unwrap());

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn append_and_scan_with_row() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_rows");
        let nested_row_type = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let deep_inner_row_type = DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
        let deep_row_type =
            DataTypes::row(vec![DataField::new("inner", deep_inner_row_type, None)]);

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", nested_row_type)
            .column("deep", deep_row_type)
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut nested1 = GenericRow::new(2);
        nested1.set_field(0, 42_i32);
        nested1.set_field(1, "hello");
        let mut deep_inner1 = GenericRow::new(1);
        deep_inner1.set_field(0, 99_i32);
        let mut deep1 = GenericRow::new(1);
        deep1.set_field(0, Datum::Row(Box::new(deep_inner1)));

        let mut row1 = GenericRow::new(3);
        row1.set_field(0, 1_i32);
        row1.set_field(1, Datum::Row(Box::new(nested1)));
        row1.set_field(2, Datum::Row(Box::new(deep1)));

        let mut nested2 = GenericRow::new(2);
        nested2.set_field(0, 7_i32);
        nested2.set_field(1, Datum::Null);

        let mut row2 = GenericRow::new(3);
        row2.set_field(0, 2_i32);
        row2.set_field(1, Datum::Row(Box::new(nested2)));
        row2.set_field(2, Datum::Null);

        let mut deep_inner3 = GenericRow::new(1);
        deep_inner3.set_field(0, -1_i32);
        let mut deep3 = GenericRow::new(1);
        deep3.set_field(0, Datum::Row(Box::new(deep_inner3)));

        let mut row3 = GenericRow::new(3);
        row3.set_field(0, 3_i32);
        row3.set_field(1, Datum::Null);
        row3.set_field(2, Datum::Row(Box::new(deep3)));

        append_writer.append(&row1).expect("append row1");
        append_writer.append(&row2).expect("append row2");
        append_writer.append(&row3).expect("append row3");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 3, "expected three log records");

        let r0 = records[0].row();
        assert_eq!(r0.get_int(0).unwrap(), 1);
        let nested_r0 = r0.get_row(1).unwrap();
        assert_eq!(nested_r0.get_int(0).unwrap(), 42);
        assert_eq!(nested_r0.get_string(1).unwrap(), "hello");
        let deep_r0 = r0.get_row(2).unwrap();
        let deep_inner_r0 = deep_r0.get_row(0).unwrap();
        assert_eq!(deep_inner_r0.get_int(0).unwrap(), 99);

        let r1 = records[1].row();
        assert_eq!(r1.get_int(0).unwrap(), 2);
        let nested_r1 = r1.get_row(1).unwrap();
        assert_eq!(nested_r1.get_int(0).unwrap(), 7);
        assert!(nested_r1.is_null_at(1).unwrap());
        assert!(r1.is_null_at(2).unwrap());

        let r2 = records[2].row();
        assert_eq!(r2.get_int(0).unwrap(), 3);
        assert!(r2.is_null_at(1).unwrap());
        let deep_r2 = r2.get_row(2).unwrap();
        let deep_inner_r2 = deep_r2.get_row(0).unwrap();
        assert_eq!(deep_inner_r2.get_int(0).unwrap(), -1);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Partitioned log table with a ROW column. Confirms partition routing
    /// + ROW column encoding compose correctly across partitions.
    /// ROW column with all rich element types (decimal, date, time, timestamps,
    /// bytes, binary, float NaN/Inf, long strings) round-tripped through the
    /// log path. Confirms the wire-level encoding of `ROW<rich types>` matches
    /// what the server expects — the unit-level `test_row_all_primitives_round_trip`
    /// proves Rust↔Rust round-trip; this test proves Rust→server→Rust.
    #[tokio::test]
    async fn append_and_scan_with_row_rich_types() {
        fn assert_f32_special(actual: f32, expected: f32) {
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN");
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert_eq!(actual.signum(), expected.signum());
            } else {
                assert!((actual - expected).abs() < f32::EPSILON);
            }
        }

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_row_rich_types");

        let row_type_owned = DataTypes::row(vec![
            DataField::new("f_bool", DataTypes::boolean(), None),
            DataField::new("f_int", DataTypes::int(), None),
            DataField::new("f_long", DataTypes::bigint(), None),
            DataField::new("f_float", DataTypes::float(), None),
            DataField::new("f_double", DataTypes::double(), None),
            DataField::new("f_str", DataTypes::string(), None),
            DataField::new("f_bytes", DataTypes::bytes(), None),
            DataField::new("f_decimal", DataTypes::decimal(10, 2), None),
            DataField::new("f_date", DataTypes::date(), None),
            DataField::new("f_time", DataTypes::time_with_precision(3), None),
            DataField::new("f_ts_ntz", DataTypes::timestamp_with_precision(6), None),
            DataField::new("f_ts_ltz", DataTypes::timestamp_ltz_with_precision(6), None),
            DataField::new("f_binary_fixed", DataTypes::binary(4), None),
            DataField::new("f_array_int", DataTypes::array(DataTypes::int()), None),
        ]);

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", row_type_owned)
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut nested1 = GenericRow::new(14);
        nested1.set_field(0, true);
        nested1.set_field(1, 100_000_i32);
        nested1.set_field(2, 9_876_543_210_i64);
        nested1.set_field(3, f32::INFINITY);
        nested1.set_field(4, f64::NAN);
        nested1.set_field(5, "hello world");
        nested1.set_field(6, b"binary".as_slice());
        nested1.set_field(7, Decimal::from_unscaled_long(12345, 10, 2).unwrap());
        nested1.set_field(8, Datum::Date(Date::new(20476)));
        nested1.set_field(9, Datum::Time(Time::new(36_827_123)));
        nested1.set_field(
            10,
            Datum::TimestampNtz(TimestampNtz::new(1_769_163_227_123)),
        );
        nested1.set_field(
            11,
            Datum::TimestampLtz(TimestampLtz::new(1_769_163_227_456)),
        );
        nested1.set_field(12, b"\x01\x02\x03\x04".as_slice());
        nested1.set_field(13, make_int_array(&[Some(7), None, Some(11)]));

        let mut row1 = GenericRow::new(2);
        row1.set_field(0, 1_i32);
        row1.set_field(1, Datum::Row(Box::new(nested1)));

        let mut row2 = GenericRow::new(2);
        row2.set_field(0, 2_i32);
        row2.set_field(1, Datum::Null);

        append_writer.append(&row1).expect("append row1");
        append_writer.append(&row2).expect("append row2");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 2);

        let r0 = records[0].row();
        assert_eq!(r0.get_int(0).unwrap(), 1);
        let nested = r0.get_row(1).unwrap();
        assert!(nested.get_boolean(0).unwrap());
        assert_eq!(nested.get_int(1).unwrap(), 100_000);
        assert_eq!(nested.get_long(2).unwrap(), 9_876_543_210);
        assert_f32_special(nested.get_float(3).unwrap(), f32::INFINITY);
        assert!(nested.get_double(4).unwrap().is_nan());
        assert_eq!(nested.get_string(5).unwrap(), "hello world");
        assert_eq!(nested.get_bytes(6).unwrap(), b"binary");
        assert_eq!(
            nested.get_decimal(7, 10, 2).unwrap(),
            Decimal::from_unscaled_long(12345, 10, 2).unwrap(),
        );
        assert_eq!(nested.get_date(8).unwrap().get_inner(), 20476);
        assert_eq!(nested.get_time(9).unwrap().get_inner(), 36_827_123);
        assert_eq!(
            nested.get_timestamp_ntz(10, 6).unwrap().get_millisecond(),
            1_769_163_227_123,
        );
        assert_eq!(
            nested
                .get_timestamp_ltz(11, 6)
                .unwrap()
                .get_epoch_millisecond(),
            1_769_163_227_456,
        );
        assert_eq!(nested.get_binary(12, 4).unwrap(), b"\x01\x02\x03\x04");
        let arr = nested.get_array(13).unwrap();
        assert_eq!(arr.size(), 3);
        assert_eq!(arr.get_int(0).unwrap(), 7);
        assert!(arr.is_null_at(1));
        assert_eq!(arr.get_int(2).unwrap(), 11);

        let r1 = records[1].row();
        assert_eq!(r1.get_int(0).unwrap(), 2);
        assert!(r1.is_null_at(1).unwrap());

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Projection over a log table with ROW columns. Specifically tests that
    /// `ProjectedRow::get_row` (added by this PR) works end-to-end against the
    /// server — without this, the projection code path for ROW would have zero
    /// integration coverage.
    #[tokio::test]
    async fn append_and_scan_with_row_projection() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_row_projection");

        let row_type = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", row_type)
            .column("extra", DataTypes::string())
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut nested = GenericRow::new(2);
        nested.set_field(0, 42_i32);
        nested.set_field(1, "hello");

        let mut row = GenericRow::new(3);
        row.set_field(0, 7_i32);
        row.set_field(1, Datum::Row(Box::new(nested)));
        row.set_field(2, "ignore-me");
        append_writer.append(&row).expect("append");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| {
            scan.project_by_name(&["nested", "id"])
                .expect("project failed")
        })
        .await;
        assert_eq!(records.len(), 1);

        let r0 = records[0].row();
        let projected_nested = r0.get_row(0).expect("get_row over projection");
        assert_eq!(projected_nested.get_int(0).unwrap(), 42);
        assert_eq!(projected_nested.get_string(1).unwrap(), "hello");
        assert_eq!(r0.get_int(1).unwrap(), 7);

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn append_and_scan_with_array_rich_types() {
        fn assert_f32_special(actual: f32, expected: f32) {
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN");
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert_eq!(actual.signum(), expected.signum());
            } else {
                assert!((actual - expected).abs() < f32::EPSILON);
            }
        }

        fn assert_f64_special(actual: f64, expected: f64) {
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN");
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert_eq!(actual.signum(), expected.signum());
            } else {
                assert!((actual - expected).abs() < f64::EPSILON);
            }
        }

        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("Failed to get admin");

        let table_path = TablePath::new("fluss", "test_log_arrays_rich_types");

        // Compact types: DECIMAL(10,2) precision<=18, TIMESTAMP(6) precision<=3 for millis
        let dec_compact = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        let ts_compact = TimestampNtz::from_millis_nanos(1769163227123, 456000).unwrap();

        // Non-compact types: DECIMAL(22,5) precision>18, TIMESTAMP(9) precision>3
        let dec_big = Decimal::from_unscaled_bytes(&[66, 237, 18, 59, 11, 216, 31, 4, 244], 22, 5)
            .expect("big decimal");
        let ts_nano = TimestampNtz::from_millis_nanos(1769163227123, 999_999).unwrap();

        let d = Date::new(20476);
        let t = Time::new(36827123);
        let elem_bytes = &[0_u8, 1, 2, 255];
        let fixed_a: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let fixed_b: Vec<u8> = vec![0x01, 0x02, 0x03, 0x04];

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("arr_bytes", DataTypes::array(DataTypes::bytes()))
            .column("arr_date", DataTypes::array(DataTypes::date()))
            .column(
                "arr_time",
                DataTypes::array(DataTypes::time_with_precision(3)),
            )
            .column(
                "arr_ts_compact",
                DataTypes::array(DataTypes::timestamp_with_precision(6)),
            )
            .column(
                "arr_ts_nano",
                DataTypes::array(DataTypes::timestamp_with_precision(9)),
            )
            .column(
                "arr_decimal_compact",
                DataTypes::array(DataTypes::decimal(10, 2)),
            )
            .column(
                "arr_decimal_big",
                DataTypes::array(DataTypes::decimal(22, 5)),
            )
            .column("arr_long_str", DataTypes::array(DataTypes::string()))
            .column("arr_float", DataTypes::array(DataTypes::float()))
            .column("arr_double", DataTypes::array(DataTypes::double()))
            .column("arr_binary", DataTypes::array(DataTypes::binary(4)))
            .build()
            .expect("Failed to build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .build()
            .expect("Failed to build table descriptor");

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

        let mut row = GenericRow::new(12);
        row.set_field(0, 1_i32);

        // col 1: arr_bytes — binary with null element
        let arr_bytes = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::bytes());
            w.write_binary_bytes(0, elem_bytes);
            w.set_null_at(1);
            w.complete().expect("arr_bytes")
        };
        row.set_field(1, arr_bytes);

        // col 2: arr_date
        let arr_date = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::date());
            w.write_date(0, d);
            w.set_null_at(1);
            w.complete().expect("arr_date")
        };
        row.set_field(2, arr_date);

        // col 3: arr_time
        let arr_time = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::time_with_precision(3));
            w.write_time(0, t);
            w.set_null_at(1);
            w.complete().expect("arr_time")
        };
        row.set_field(3, arr_time);

        // col 4: arr_ts_compact — compact timestamp (precision 6, millis+nanos)
        let arr_ts_compact = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::timestamp_with_precision(6));
            w.write_timestamp_ntz(0, &ts_compact, 6);
            w.set_null_at(1);
            w.complete().expect("arr_ts_compact")
        };
        row.set_field(4, arr_ts_compact);

        // col 5: arr_ts_nano — non-compact timestamp (precision 9)
        let arr_ts_nano = {
            let mut w = FlussArrayWriter::new(1, &DataTypes::timestamp_with_precision(9));
            w.write_timestamp_ntz(0, &ts_nano, 9);
            w.complete().expect("arr_ts_nano")
        };
        row.set_field(5, arr_ts_nano);

        // col 6: arr_decimal_compact — compact decimal (precision 10)
        let arr_decimal_compact = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::decimal(10, 2));
            w.write_decimal(0, &dec_compact, 10);
            w.set_null_at(1);
            w.complete().expect("arr_decimal_compact")
        };
        row.set_field(6, arr_decimal_compact);

        // col 7: arr_decimal_big — non-compact decimal (precision 22)
        let arr_decimal_big = {
            let mut w = FlussArrayWriter::new(1, &DataTypes::decimal(22, 5));
            w.write_decimal(0, &dec_big, 22);
            w.complete().expect("arr_decimal_big")
        };
        row.set_field(7, arr_decimal_big);

        // col 8: arr_long_str — heap-backed strings (>= 8 bytes)
        let arr_long_str = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::string());
            w.write_string(0, "abcdefghi");
            w.write_string(1, "longstring_here");
            w.complete().expect("arr_long_str")
        };
        row.set_field(8, arr_long_str);

        // col 9: arr_float — IEEE 754 specials
        let arr_float = {
            let mut w = FlussArrayWriter::new(3, &DataTypes::float());
            w.write_float(0, f32::NAN);
            w.write_float(1, f32::INFINITY);
            w.write_float(2, f32::NEG_INFINITY);
            w.complete().expect("arr_float")
        };
        row.set_field(9, arr_float);

        // col 10: arr_double — IEEE 754 specials
        let arr_double = {
            let mut w = FlussArrayWriter::new(3, &DataTypes::double());
            w.write_double(0, f64::NAN);
            w.write_double(1, f64::INFINITY);
            w.write_double(2, f64::NEG_INFINITY);
            w.complete().expect("arr_double")
        };
        row.set_field(10, arr_double);

        // col 11: arr_binary — fixed-size binary(4)
        let arr_binary = {
            let mut w = FlussArrayWriter::new(2, &DataTypes::binary(4));
            w.write_binary_bytes(0, &fixed_a);
            w.write_binary_bytes(1, &fixed_b);
            w.complete().expect("arr_binary")
        };
        row.set_field(11, arr_binary);

        append_writer.append(&row).expect("append");
        append_writer.flush().await.expect("Failed to flush");

        let records = scan_table(&table, |scan| scan).await;
        assert_eq!(records.len(), 1);
        let r = records[0].row();

        // Verify arr_bytes
        let ab = r.get_array(1).unwrap();
        assert_eq!(ab.size(), 2);
        assert_eq!(ab.get_binary(0).unwrap(), elem_bytes);
        assert!(ab.is_null_at(1));

        // Verify arr_date
        let ad = r.get_array(2).unwrap();
        assert_eq!(ad.size(), 2);
        assert_eq!(ad.get_date(0).unwrap().get_inner(), d.get_inner());
        assert!(ad.is_null_at(1));

        // Verify arr_time
        let at = r.get_array(3).unwrap();
        assert_eq!(at.size(), 2);
        assert_eq!(at.get_time(0).unwrap().get_inner(), t.get_inner());
        assert!(at.is_null_at(1));

        // Verify arr_ts_compact
        let ats = r.get_array(4).unwrap();
        assert_eq!(ats.size(), 2);
        let read_ts_compact = ats.get_timestamp_ntz(0, 6).unwrap();
        assert_eq!(
            read_ts_compact.get_millisecond(),
            ts_compact.get_millisecond()
        );
        assert_eq!(
            read_ts_compact.get_nano_of_millisecond(),
            ts_compact.get_nano_of_millisecond()
        );
        assert!(ats.is_null_at(1));

        // Verify arr_ts_nano
        let ats_nano = r.get_array(5).unwrap();
        assert_eq!(ats_nano.size(), 1);
        let read_ts_nano = ats_nano.get_timestamp_ntz(0, 9).unwrap();
        assert_eq!(read_ts_nano.get_millisecond(), ts_nano.get_millisecond());
        assert_eq!(
            read_ts_nano.get_nano_of_millisecond(),
            ts_nano.get_nano_of_millisecond()
        );

        // Verify arr_decimal_compact
        let adc = r.get_array(6).unwrap();
        assert_eq!(adc.size(), 2);
        assert_eq!(adc.get_decimal(0, 10, 2).unwrap(), dec_compact);
        assert!(adc.is_null_at(1));

        // Verify arr_decimal_big
        let adb = r.get_array(7).unwrap();
        assert_eq!(adb.size(), 1);
        assert_eq!(adb.get_decimal(0, 22, 5).unwrap(), dec_big);

        // Verify arr_long_str
        let als = r.get_array(8).unwrap();
        assert_eq!(als.size(), 2);
        assert_eq!(als.get_string(0).unwrap(), "abcdefghi");
        assert_eq!(als.get_string(1).unwrap(), "longstring_here");

        // Verify arr_float — IEEE 754 specials
        let af = r.get_array(9).unwrap();
        assert_eq!(af.size(), 3);
        assert_f32_special(af.get_float(0).unwrap(), f32::NAN);
        assert_f32_special(af.get_float(1).unwrap(), f32::INFINITY);
        assert_f32_special(af.get_float(2).unwrap(), f32::NEG_INFINITY);

        // Verify arr_double — IEEE 754 specials
        let adbl = r.get_array(10).unwrap();
        assert_eq!(adbl.size(), 3);
        assert_f64_special(adbl.get_double(0).unwrap(), f64::NAN);
        assert_f64_special(adbl.get_double(1).unwrap(), f64::INFINITY);
        assert_f64_special(adbl.get_double(2).unwrap(), f64::NEG_INFINITY);

        // Verify arr_binary — fixed-size binary(4)
        let fb: FlussArray = r.get_array(11).unwrap();
        assert_eq!(fb.size(), 2);
        assert_eq!(fb.get_binary(0).unwrap(), fixed_a.as_slice());
        assert_eq!(fb.get_binary(1).unwrap(), fixed_b.as_slice());

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
