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
mod dynamic_batch_size_test {
    use crate::integration::utils::{create_table, get_shared_cluster, wait_for_table_ready};
    use fluss::client::FlussConnection;
    use fluss::config::Config;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::{Datum, GenericRow};

    fn make_config(
        bootstrap_servers: &str,
        batch_size: i32,
        dynamic_enabled: bool,
        dynamic_min: i32,
    ) -> Config {
        Config {
            bootstrap_servers: bootstrap_servers.to_string(),
            writer_acks: "all".to_string(),
            writer_batch_size: batch_size,
            writer_dynamic_batch_size_enabled: dynamic_enabled,
            writer_dynamic_batch_size_min: dynamic_min,
            ..Config::default()
        }
    }

    fn log_table_descriptor() -> TableDescriptor {
        TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("payload", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table descriptor")
    }

    fn make_row(id: i32, payload: &str) -> GenericRow<'static> {
        let mut row = GenericRow::new(2);
        row.set_field(0, Datum::Int32(id));
        row.set_field(1, Datum::String(payload.to_string().into()));
        row
    }

    /// Many small rows should cause the estimator to shrink the batch size toward min.
    /// Verifies that all writes succeed end-to-end with dynamic sizing enabled.
    #[tokio::test]
    async fn small_rows_shrink_batch_size() {
        let cluster = get_shared_cluster();
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024, // max = 256 KB
            true,
            4 * 1024, // min = 4 KB
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");

        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_small_rows");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Write many tiny rows — each well below 50% of the batch size, so
        // the estimator should shrink the target toward min after each drain.
        for i in 0..200 {
            let row = make_row(i, "x");
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");
    }

    /// Rows close to the batch size should cause the estimator to grow toward max.
    /// Verifies that all writes succeed end-to-end with dynamic sizing enabled.
    #[tokio::test]
    async fn large_rows_grow_batch_size() {
        let cluster = get_shared_cluster();
        let max_batch = 256 * 1024i32; // 256 KB
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            max_batch,
            true,
            4 * 1024, // min = 4 KB
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");

        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_large_rows");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        // Write rows with a payload that fills well above 80% of the batch.
        // Repeat to give the estimator multiple drain cycles to grow.
        let large_payload = "A".repeat(220 * 1024); // ~220 KB per row
        for i in 0..5 {
            let row = make_row(i, &large_payload);
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");
    }

    /// With dynamic sizing disabled, the writer should use the static writer_batch_size
    /// regardless of how large or small the rows are. Verifies writes succeed.
    #[tokio::test]
    async fn disabled_keeps_static_batch_size() {
        let cluster = get_shared_cluster();
        let config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024,
            false, // disabled
            4 * 1024,
        );
        let connection = FlussConnection::new(config)
            .await
            .expect("Failed to connect");

        let admin = connection.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_disabled");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");
        let writer = table
            .new_append()
            .expect("Failed to create append")
            .create_writer()
            .expect("Failed to create writer");

        for i in 0..100 {
            let row = make_row(i, "static");
            writer.append(&row).expect("Failed to append row");
        }
        writer.flush().await.expect("Failed to flush");
    }

    /// Multiple concurrent writers to the same table should not corrupt the estimator.
    /// Each writer uses its own connection; all writes must succeed.
    #[tokio::test]
    async fn concurrent_writers_dont_corrupt_state() {
        let cluster = get_shared_cluster();

        // Create the table using a shared admin connection.
        let setup_config = make_config(
            cluster.plaintext_bootstrap_servers(),
            256 * 1024,
            true,
            4 * 1024,
        );
        let setup_conn = FlussConnection::new(setup_config)
            .await
            .expect("Failed to connect for setup");
        let admin = setup_conn.get_admin().expect("Failed to get admin");
        let table_path = TablePath::new("fluss", "test_dynamic_concurrent");
        create_table(&admin, &table_path, &log_table_descriptor()).await;
        wait_for_table_ready(&admin, &table_path).await;

        let bootstrap = cluster.plaintext_bootstrap_servers().to_string();
        let table_path_clone = table_path.clone();

        // Spawn 4 concurrent writer tasks, each with its own connection.
        let mut handles = Vec::new();
        for writer_id in 0..4usize {
            let servers = bootstrap.clone();
            let path = table_path_clone.clone();
            let handle = tokio::spawn(async move {
                let config = Config {
                    bootstrap_servers: servers,
                    writer_acks: "all".to_string(),
                    writer_batch_size: 256 * 1024,
                    writer_dynamic_batch_size_enabled: true,
                    writer_dynamic_batch_size_min: 4 * 1024,
                    ..Config::default()
                };
                let conn = FlussConnection::new(config)
                    .await
                    .expect("Failed to connect");
                let table = conn.get_table(&path).await.expect("Failed to get table");
                let writer = table
                    .new_append()
                    .expect("Failed to create append")
                    .create_writer()
                    .expect("Failed to create writer");

                for i in 0..50 {
                    let row = make_row((writer_id * 50 + i) as i32, "concurrent");
                    writer.append(&row).expect("Failed to append");
                }
                writer.flush().await.expect("Failed to flush");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Writer task panicked");
        }
    }
}
