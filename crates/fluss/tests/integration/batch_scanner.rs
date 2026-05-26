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
mod batch_scanner_test {
    use crate::integration::utils::{create_table, get_shared_cluster};
    use arrow::array::record_batch;
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use std::time::Duration;

    /// End-to-end check that BatchScanner returns the appended rows on first
    /// poll and `None` on the next, honoring the configured limit.
    #[tokio::test]
    async fn batch_scanner_returns_appended_rows_then_none() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_log");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            // Single bucket so a single BatchScanner sees every row.
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");

        let batch = record_batch!(
            ("c1", Int32, [1, 2, 3, 4, 5]),
            ("c2", Utf8, ["a", "b", "c", "d", "e"])
        )
        .unwrap();
        writer.append_arrow_batch(batch).expect("append batch");
        writer.flush().await.expect("flush");

        // Give the server a moment to commit and make the records readable.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let table_info = table.get_table_info();
        let bucket = TableBucket::new(table_info.table_id, 0);

        let mut batch_scanner = table
            .new_scan()
            .limit(3)
            .expect("limit")
            .create_batch_scanner(bucket.clone())
            .await
            .expect("create batch scanner");

        let first = batch_scanner
            .poll_batch()
            .await
            .expect("poll")
            .expect("first batch should be Some");

        assert_eq!(first.bucket(), &bucket);
        // The server may return fewer rows than the limit on the first call,
        // but must never exceed it.
        assert!(
            first.num_records() > 0 && first.num_records() <= 3,
            "expected 1..=3 records, got {}",
            first.num_records()
        );

        let second = batch_scanner
            .poll_batch()
            .await
            .expect("second poll succeeds");
        assert!(second.is_none(), "second poll must return None");
    }

    /// A bucket id outside the table's bucket range should be rejected by the
    /// scanner before any RPC is made.
    #[tokio::test]
    async fn batch_scanner_requires_matching_table_id() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_table_id");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");

        // Bucket with a wrong table_id — must fail without hitting the server.
        let bogus_bucket = TableBucket::new(table.get_table_info().table_id + 9999, 0);

        let result = table
            .new_scan()
            .limit(1)
            .expect("limit")
            .create_batch_scanner(bogus_bucket)
            .await;
        assert!(
            result.is_err(),
            "batch scanner must reject mismatched table_id"
        );
    }

    /// `.limit(n)` must reject non-positive values before any scanner is built.
    #[tokio::test]
    async fn batch_scanner_rejects_non_positive_limit() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");

        let table_path = TablePath::new("fluss", "test_batch_scanner_bad_limit");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["c1".to_string()])
            .build()
            .expect("descriptor");
        create_table(&admin, &table_path, &descriptor).await;

        let table = connection.get_table(&table_path).await.expect("table");
        assert!(table.new_scan().limit(0).is_err());
        assert!(table.new_scan().limit(-5).is_err());
    }
}
