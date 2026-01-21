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
mod kv_table_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::create_table;
    use fluss::client::UpsertWriter;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::{GenericRow, InternalRow};
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
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        std::thread::spawn(move || {
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

    fn make_key(id: i32) -> GenericRow<'static> {
        let mut row = GenericRow::new();
        row.set_field(0, id);
        row
    }

    #[tokio::test]
    async fn upsert_and_lookup() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new(
            "fluss".to_string(),
            "test_upsert_and_lookup".to_string(),
        );

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("age", DataTypes::bigint())
                    .primary_key(vec!["id".to_string()])
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

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let test_data = [(1, "Verso", 32i64), (2, "Noco", 25), (3, "Esquie", 35)];

        // Upsert rows
        for (id, name, age) in &test_data {
            let mut row = GenericRow::new();
            row.set_field(0, *id);
            row.set_field(1, *name);
            row.set_field(2, *age);
            upsert_writer
                .upsert(&row)
                .await
                .expect("Failed to upsert row");
        }

        // Lookup records
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        // Verify lookup results
        for (id, expected_name, expected_age) in &test_data {
            let result = lookuper
                .lookup(&make_key(*id))
                .await
                .expect("Failed to lookup");
            let row = result
                .get_single_row()
                .expect("Failed to get row")
                .expect("Row should exist");

            assert_eq!(row.get_int(0), *id, "id mismatch");
            assert_eq!(row.get_string(1), *expected_name, "name mismatch");
            assert_eq!(row.get_long(2), *expected_age, "age mismatch");
        }
    }

    #[tokio::test]
    async fn update_existing_record() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path =
            TablePath::new("fluss".to_string(), "test_update_existing_record".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("score", DataTypes::bigint())
                    .primary_key(vec!["id".to_string()])
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

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        // Insert initial record
        let mut row = GenericRow::new();
        row.set_field(0, 1);
        row.set_field(1, "Flash");
        row.set_field(2, 123456789012i64);
        upsert_writer
            .upsert(&row)
            .await
            .expect("Failed to upsert initial row");

        // Verify initial record
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup");
        let found_row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(found_row.get_long(2), row.get_long(2), "Expected initial score to be 123456789012i64");

        // Update the record with new score
        let mut updated_row = GenericRow::new();
        updated_row.set_field(0, 1);
        updated_row.set_field(1, "Flash");
        updated_row.set_field(2, 987654321098i64);
        upsert_writer
            .upsert(&updated_row)
            .await
            .expect("Failed to upsert updated row");

        // Verify the update
        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup after update");
        let found_row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(
            found_row.get_long(2),
            updated_row.get_long(2),
            "Score should be updated"
        );
        assert_eq!(
            found_row.get_string(1),
            updated_row.get_string(1),
            "Name should remain unchanged"
        );
    }

    #[tokio::test]
    async fn delete_record() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_delete_record".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("data", DataTypes::string())
                    .primary_key(vec!["id".to_string()])
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

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        // Insert records
        for i in 1..=3 {
            let mut row = GenericRow::new();
            row.set_field(0, i);
            let data = format!("data{}", i);
            row.set_field(1, data.as_str());
            upsert_writer.upsert(&row).await.expect("Failed to upsert");
        }

        // Verify records exist
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        for i in 1..=3 {
            let result = lookuper
                .lookup(&make_key(i))
                .await
                .expect("Failed to lookup");
            assert!(
                result.get_single_row().expect("Failed to get row").is_some(),
                "Record {} should exist before delete",
                i
            );
        }

        // Delete record with id=2
        let mut delete_row = GenericRow::new();
        delete_row.set_field(0, 2);
        delete_row.set_field(1, "");
        upsert_writer
            .delete(&delete_row)
            .await
            .expect("Failed to delete");

        // Verify deletion
        let result = lookuper
            .lookup(&make_key(2))
            .await
            .expect("Failed to lookup deleted record");
        assert!(
            result.get_single_row().expect("Failed to get row").is_none(),
            "Record 2 should not exist after delete"
        );

        // Verify other records still exist
        for i in [1, 3] {
            let result = lookuper
                .lookup(&make_key(i))
                .await
                .expect("Failed to lookup");
            assert!(
                result.get_single_row().expect("Failed to get row").is_some(),
                "Record {} should still exist after deleting record 2",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_lookup_non_existent_key() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path =
            TablePath::new("fluss".to_string(), "test_lookup_non_existent".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("value", DataTypes::string())
                    .primary_key(vec!["id".to_string()])
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

        // Insert one record
        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let mut row = GenericRow::new();
        row.set_field(0, 1);
        row.set_field(1, "exists");
        upsert_writer.upsert(&row).await.expect("Failed to upsert");

        // Lookup non-existent key
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        let result = lookuper
            .lookup(&make_key(999))
            .await
            .expect("Failed to lookup non-existent key");
        assert!(
            result.get_single_row().expect("Failed to get row").is_none(),
            "Non-existent key should return None"
        );
    }

    #[tokio::test]
    async fn test_multiple_primary_key_columns() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path =
            TablePath::new("fluss".to_string(), "test_composite_pk".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("user_id", DataTypes::int())
                    .column("score", DataTypes::bigint())
                    .primary_key(vec!["region".to_string(), "user_id".to_string()])
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

        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        // Insert records with composite keys
        let test_data = [
            ("US", 1, 100i64),
            ("US", 2, 200i64),
            ("EU", 1, 150i64),
            ("EU", 2, 250i64),
        ];

        for (region, user_id, score) in &test_data {
            let mut row = GenericRow::new();
            row.set_field(0, *region);
            row.set_field(1, *user_id);
            row.set_field(2, *score);
            upsert_writer.upsert(&row).await.expect("Failed to upsert");
        }

        // Lookup with composite key
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        // Lookup (US, 1) - should return score 100
        let mut key = GenericRow::new();
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_long(2), 100, "Score for (US, 1) should be 100");

        // Lookup (EU, 2) - should return score 250
        let mut key = GenericRow::new();
        key.set_field(0, "EU");
        key.set_field(1, 2);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_long(2), 250, "Score for (EU, 2) should be 250");

        // Update (US, 1) score
        let mut update_row = GenericRow::new();
        update_row.set_field(0, "US");
        update_row.set_field(1, 1);
        update_row.set_field(2, 500i64);
        upsert_writer
            .upsert(&update_row)
            .await
            .expect("Failed to update");

        // Verify update
        let mut key = GenericRow::new();
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(
            row.get_long(2),
            update_row.get_long(2),
            "Row balance should be updated"
        );
    }
}
