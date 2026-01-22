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
    use crate::integration::fluss_cluster::FlussTestingCluster;
    use crate::integration::utils::{create_table, get_cluster, start_cluster, stop_cluster};
    use fluss::client::UpsertWriter;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::{GenericRow, InternalRow};
    use std::sync::Arc;

    fn before_all() {
        start_cluster("test_kv_table", SHARED_FLUSS_CLUSTER.clone());
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        get_cluster(&SHARED_FLUSS_CLUSTER)
    }

    fn after_all() {
        stop_cluster(SHARED_FLUSS_CLUSTER.clone());
    }

    fn make_key(id: i32) -> GenericRow<'static> {
        let mut row = GenericRow::new();
        row.set_field(0, id);
        row.set_field(1, "");
        row.set_field(2, 0i64);
        row
    }

    #[tokio::test]
    async fn upsert_delete_and_lookup() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_upsert_and_lookup".to_string());

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

        // Update the record with new age
        let mut updated_row = GenericRow::new();
        updated_row.set_field(0, 1);
        updated_row.set_field(1, "Verso");
        updated_row.set_field(2, 33i64);
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
            "Age should be updated"
        );
        assert_eq!(
            found_row.get_string(1),
            updated_row.get_string(1),
            "Name should remain unchanged"
        );

        // Delete record with id=1
        let mut delete_row = GenericRow::new();
        delete_row.set_field(0, 1);
        delete_row.set_field(1, "");
        delete_row.set_field(2, 0i64);
        upsert_writer
            .delete(&delete_row)
            .await
            .expect("Failed to delete");

        // Verify deletion
        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup deleted record");
        assert!(
            result
                .get_single_row()
                .expect("Failed to get row")
                .is_none(),
            "Record 1 should not exist after delete"
        );

        // Verify other records still exist
        for i in [2, 3] {
            let result = lookuper
                .lookup(&make_key(i))
                .await
                .expect("Failed to lookup");
            assert!(
                result
                    .get_single_row()
                    .expect("Failed to get row")
                    .is_some(),
                "Record {} should still exist after deleting record 1",
                i
            );
        }

        // Lookup non-existent key
        let result = lookuper
            .lookup(&make_key(999))
            .await
            .expect("Failed to lookup non-existent key");
        assert!(
            result
                .get_single_row()
                .expect("Failed to get row")
                .is_none(),
            "Non-existent key should return None"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn composite_primary_keys() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_composite_pk".to_string());

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
            "Row score should be updated"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    #[tokio::test]
    async fn partial_update() {
        use fluss::row::Datum;

        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_partial_update".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("age", DataTypes::bigint())
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

        // Insert initial record with all columns
        let table_upsert = table.new_upsert().expect("Failed to create upsert");
        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        let mut row = GenericRow::new();
        row.set_field(0, 1);
        row.set_field(1, "Verso");
        row.set_field(2, 32i64);
        row.set_field(3, 6942i64);
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

        assert_eq!(found_row.get_int(0), 1);
        assert_eq!(found_row.get_string(1), "Verso");
        assert_eq!(found_row.get_long(2), 32i64);
        assert_eq!(found_row.get_long(3), 6942i64);

        // Create partial update writer to update only score column
        let partial_upsert = table_upsert
            .partial_update_with_column_names(&["id", "score"])
            .expect("Failed to create TableUpsert with partial update");
        let mut partial_writer = partial_upsert
            .create_writer()
            .expect("Failed to create UpsertWriter with partial write");

        // Update only the score column
        let mut partial_row = GenericRow::new();
        partial_row.set_field(0, 1);
        partial_row.set_field(1, Datum::Null); // not in partial update column
        partial_row.set_field(2, Datum::Null); // not in partial update column
        partial_row.set_field(3, 420i64);
        partial_writer
            .upsert(&partial_row)
            .await
            .expect("Failed to upsert");

        // Verify partial update - name and age should remain unchanged
        let result = lookuper
            .lookup(&make_key(1))
            .await
            .expect("Failed to lookup after partial update");
        let found_row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");

        assert_eq!(found_row.get_int(0), 1, "id should remain 1");
        assert_eq!(
            found_row.get_string(1),
            "Verso",
            "name should remain unchanged"
        );
        assert_eq!(found_row.get_long(2), 32, "age should remain unchanged");
        assert_eq!(found_row.get_long(3), 420, "score should be updated to 420");

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
