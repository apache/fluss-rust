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
    use crate::integration::utils::{
        create_partitions, create_table, get_cluster, start_cluster, stop_cluster,
    };
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
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
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
            let mut row = GenericRow::new(3);
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
        let mut updated_row = GenericRow::new(3);
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
        let mut delete_row = GenericRow::new(3);
        delete_row.set_field(0, 1);
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
            let mut row = GenericRow::new(3);
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
        let mut key = GenericRow::new(3);
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_long(2), 100, "Score for (US, 1) should be 100");

        // Lookup (EU, 2) - should return score 250
        let mut key = GenericRow::new(3);
        key.set_field(0, "EU");
        key.set_field(1, 2);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_long(2), 250, "Score for (EU, 2) should be 250");

        // Update (US, 1) score
        let mut update_row = GenericRow::new(3);
        update_row.set_field(0, "US");
        update_row.set_field(1, 1);
        update_row.set_field(2, 500i64);
        upsert_writer
            .upsert(&update_row)
            .await
            .expect("Failed to update");

        // Verify update
        let mut key = GenericRow::new(3);
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

        let mut row = GenericRow::new(4);
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
        let mut partial_row = GenericRow::new(4);
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

    #[tokio::test]
    async fn partitioned_table_upsert_and_lookup() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path =
            TablePath::new("fluss".to_string(), "test_partitioned_kv_table".to_string());

        // Create a partitioned KV table with region as partition key
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("region", DataTypes::string())
                    .column("user_id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("score", DataTypes::bigint())
                    .primary_key(vec!["region".to_string(), "user_id".to_string()])
                    .build()
                    .expect("Failed to build schema"),
            )
            .partitioned_by(vec!["region".to_string()])
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        // Create partitions for each region before inserting data
        create_partitions(&admin, &table_path, "region", &["US", "EU", "APAC"]).await;

        let connection = cluster.get_fluss_connection().await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table");

        let table_upsert = table.new_upsert().expect("Failed to create upsert");

        let mut upsert_writer = table_upsert
            .create_writer()
            .expect("Failed to create writer");

        // Insert records with different partitions
        let test_data = [
            ("US", 1, "Gustave", 100i64),
            ("US", 2, "Lune", 200i64),
            ("EU", 1, "Sciel", 150i64),
            ("EU", 2, "Maelle", 250i64),
            ("APAC", 1, "Noco", 300i64),
        ];

        for (region, user_id, name, score) in &test_data {
            let mut row = GenericRow::new(4);
            row.set_field(0, *region);
            row.set_field(1, *user_id);
            row.set_field(2, *name);
            row.set_field(3, *score);
            upsert_writer.upsert(&row).await.expect("Failed to upsert");
        }

        // Create lookuper
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        // Lookup records - the lookup key includes partition key columns
        for (region, user_id, expected_name, expected_score) in &test_data {
            let mut key = GenericRow::new(4);
            key.set_field(0, *region);
            key.set_field(1, *user_id);

            let result = lookuper.lookup(&key).await.expect("Failed to lookup");
            let row = result
                .get_single_row()
                .expect("Failed to get row")
                .expect("Row should exist");

            assert_eq!(row.get_string(0), *region, "region mismatch");
            assert_eq!(row.get_int(1), *user_id, "user_id mismatch");
            assert_eq!(row.get_string(2), *expected_name, "name mismatch");
            assert_eq!(row.get_long(3), *expected_score, "score mismatch");
        }

        // Test update within a partition
        let mut updated_row = GenericRow::new(4);
        updated_row.set_field(0, "US");
        updated_row.set_field(1, 1);
        updated_row.set_field(2, "Gustave Updated");
        updated_row.set_field(3, 999i64);
        upsert_writer
            .upsert(&updated_row)
            .await
            .expect("Failed to upsert updated row");

        // Verify the update
        let mut key = GenericRow::new(4);
        key.set_field(0, "US");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_string(2), "Gustave Updated");
        assert_eq!(row.get_long(3), 999);

        // Lookup in non-existent partition should return empty result
        let mut non_existent_key = GenericRow::new(4);
        non_existent_key.set_field(0, "UNKNOWN_REGION");
        non_existent_key.set_field(1, 1);
        let result = lookuper
            .lookup(&non_existent_key)
            .await
            .expect("Failed to lookup non-existent partition");
        assert!(
            result
                .get_single_row()
                .expect("Failed to get row")
                .is_none(),
            "Lookup in non-existent partition should return None"
        );

        // Delete a record within a partition
        let mut delete_key = GenericRow::new(4);
        delete_key.set_field(0, "EU");
        delete_key.set_field(1, 1);
        upsert_writer
            .delete(&delete_key)
            .await
            .expect("Failed to delete");

        // Verify deletion
        let mut key = GenericRow::new(4);
        key.set_field(0, "EU");
        key.set_field(1, 1);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        assert!(
            result
                .get_single_row()
                .expect("Failed to get row")
                .is_none(),
            "Deleted record should not exist"
        );

        // Verify other records in the same partition still exist
        let mut key = GenericRow::new(4);
        key.set_field(0, "EU");
        key.set_field(1, 2);
        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");
        assert_eq!(row.get_string(2), "Maelle");

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Integration test covering put and get operations for all supported datatypes.
    #[tokio::test]
    async fn all_supported_datatypes() {
        use fluss::row::{Date, Datum, Decimal, Time, TimestampLtz, TimestampNtz};

        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_all_datatypes".to_string());

        // Create a table with all supported primitive datatypes
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    // Primary key column
                    .column("pk_int", DataTypes::int())
                    // Boolean type
                    .column("col_boolean", DataTypes::boolean())
                    // Integer types
                    .column("col_tinyint", DataTypes::tinyint())
                    .column("col_smallint", DataTypes::smallint())
                    .column("col_int", DataTypes::int())
                    .column("col_bigint", DataTypes::bigint())
                    // Floating point types
                    .column("col_float", DataTypes::float())
                    .column("col_double", DataTypes::double())
                    // String types
                    .column("col_char", DataTypes::char(10))
                    .column("col_string", DataTypes::string())
                    // Decimal type
                    .column("col_decimal", DataTypes::decimal(10, 2))
                    // Date and time types
                    .column("col_date", DataTypes::date())
                    .column("col_time", DataTypes::time())
                    .column("col_timestamp", DataTypes::timestamp())
                    .column("col_timestamp_ltz", DataTypes::timestamp_ltz())
                    // Binary types
                    .column("col_bytes", DataTypes::bytes())
                    .column("col_binary", DataTypes::binary(20))
                    .primary_key(vec!["pk_int".to_string()])
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

        // Test data for all datatypes
        let pk_int = 1i32;
        let col_boolean = true;
        let col_tinyint = 127i8;
        let col_smallint = 32767i16;
        let col_int = 2147483647i32;
        let col_bigint = 9223372036854775807i64;
        let col_float = 3.14f32;
        let col_double = 2.718281828459045f64;
        let col_char = "hello";
        let col_string = "world of fluss rust client";
        let col_decimal = Decimal::from_unscaled_long(12345, 10, 2).unwrap(); // 123.45
        let col_date = Date::new(20476); // 2026-01-23
        let col_time = Time::new(36827123); // 10:13:47.123
        let col_timestamp = TimestampNtz::new(1769163227123); // 2026-01-23 10:13:47.123 UTC
        let col_timestamp_ltz = TimestampLtz::new(1769163227123); // 2026-01-23 10:13:47.123 UTC
        let col_bytes: &[u8] = b"binary data";
        let col_binary: &[u8] = b"fixed binary data!!!";

        // Upsert a row with all datatypes
        let mut row = GenericRow::new(17);
        row.set_field(0, pk_int);
        row.set_field(1, col_boolean);
        row.set_field(2, col_tinyint);
        row.set_field(3, col_smallint);
        row.set_field(4, col_int);
        row.set_field(5, col_bigint);
        row.set_field(6, col_float);
        row.set_field(7, col_double);
        row.set_field(8, col_char);
        row.set_field(9, col_string);
        row.set_field(10, col_decimal.clone());
        row.set_field(11, col_date);
        row.set_field(12, col_time);
        row.set_field(13, col_timestamp);
        row.set_field(14, col_timestamp_ltz);
        row.set_field(15, col_bytes);
        row.set_field(16, col_binary);

        upsert_writer
            .upsert(&row)
            .await
            .expect("Failed to upsert row with all datatypes");

        // Lookup the record
        let mut lookuper = table
            .new_lookup()
            .expect("Failed to create lookup")
            .create_lookuper()
            .expect("Failed to create lookuper");

        let mut key = GenericRow::new(17);
        key.set_field(0, pk_int);

        let result = lookuper.lookup(&key).await.expect("Failed to lookup");
        let found_row = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");

        // Verify all datatypes
        assert_eq!(found_row.get_int(0), pk_int, "pk_int mismatch");
        assert_eq!(
            found_row.get_boolean(1),
            col_boolean,
            "col_boolean mismatch"
        );
        assert_eq!(found_row.get_byte(2), col_tinyint, "col_tinyint mismatch");
        assert_eq!(
            found_row.get_short(3),
            col_smallint,
            "col_smallint mismatch"
        );
        assert_eq!(found_row.get_int(4), col_int, "col_int mismatch");
        assert_eq!(found_row.get_long(5), col_bigint, "col_bigint mismatch");
        assert!(
            (found_row.get_float(6) - col_float).abs() < f32::EPSILON,
            "col_float mismatch: expected {}, got {}",
            col_float,
            found_row.get_float(6)
        );
        assert!(
            (found_row.get_double(7) - col_double).abs() < f64::EPSILON,
            "col_double mismatch: expected {}, got {}",
            col_double,
            found_row.get_double(7)
        );
        assert_eq!(found_row.get_char(8, 10), col_char, "col_char mismatch");
        assert_eq!(found_row.get_string(9), col_string, "col_string mismatch");
        assert_eq!(
            found_row.get_decimal(10, 10, 2),
            col_decimal,
            "col_decimal mismatch"
        );
        assert_eq!(
            found_row.get_date(11).get_inner(),
            col_date.get_inner(),
            "col_date mismatch"
        );
        assert_eq!(
            found_row.get_time(12).get_inner(),
            col_time.get_inner(),
            "col_time mismatch"
        );
        assert_eq!(
            found_row.get_timestamp_ntz(13, 6).get_millisecond(),
            col_timestamp.get_millisecond(),
            "col_timestamp mismatch"
        );
        assert_eq!(
            found_row.get_timestamp_ltz(14, 6).get_epoch_millisecond(),
            col_timestamp_ltz.get_epoch_millisecond(),
            "col_timestamp_ltz mismatch"
        );
        assert_eq!(found_row.get_bytes(15), col_bytes, "col_bytes mismatch");
        assert_eq!(
            found_row.get_binary(16, 20),
            col_binary,
            "col_binary mismatch"
        );

        // Test with null values for nullable columns
        let pk_int_2 = 2i32;
        let mut row_with_nulls = GenericRow::new(17);
        row_with_nulls.set_field(0, pk_int_2);
        row_with_nulls.set_field(1, Datum::Null); // col_boolean
        row_with_nulls.set_field(2, Datum::Null); // col_tinyint
        row_with_nulls.set_field(3, Datum::Null); // col_smallint
        row_with_nulls.set_field(4, Datum::Null); // col_int
        row_with_nulls.set_field(5, Datum::Null); // col_bigint
        row_with_nulls.set_field(6, Datum::Null); // col_float
        row_with_nulls.set_field(7, Datum::Null); // col_double
        row_with_nulls.set_field(8, Datum::Null); // col_char
        row_with_nulls.set_field(9, Datum::Null); // col_string
        row_with_nulls.set_field(10, Datum::Null); // col_decimal
        row_with_nulls.set_field(11, Datum::Null); // col_date
        row_with_nulls.set_field(12, Datum::Null); // col_time
        row_with_nulls.set_field(13, Datum::Null); // col_timestamp
        row_with_nulls.set_field(14, Datum::Null); // col_timestamp_ltz
        row_with_nulls.set_field(15, Datum::Null); // col_bytes
        row_with_nulls.set_field(16, Datum::Null); // col_binary

        upsert_writer
            .upsert(&row_with_nulls)
            .await
            .expect("Failed to upsert row with nulls");

        // Lookup row with nulls
        let mut key2 = GenericRow::new(17);
        key2.set_field(0, pk_int_2);

        let result = lookuper.lookup(&key2).await.expect("Failed to lookup");
        let found_row_nulls = result
            .get_single_row()
            .expect("Failed to get row")
            .expect("Row should exist");

        // Verify all nullable columns are null
        assert_eq!(found_row_nulls.get_int(0), pk_int_2, "pk_int mismatch");
        assert!(found_row_nulls.is_null_at(1), "col_boolean should be null");
        assert!(found_row_nulls.is_null_at(2), "col_tinyint should be null");
        assert!(found_row_nulls.is_null_at(3), "col_smallint should be null");
        assert!(found_row_nulls.is_null_at(4), "col_int should be null");
        assert!(found_row_nulls.is_null_at(5), "col_bigint should be null");
        assert!(found_row_nulls.is_null_at(6), "col_float should be null");
        assert!(found_row_nulls.is_null_at(7), "col_double should be null");
        assert!(found_row_nulls.is_null_at(8), "col_char should be null");
        assert!(found_row_nulls.is_null_at(9), "col_string should be null");
        assert!(found_row_nulls.is_null_at(10), "col_decimal should be null");
        assert!(found_row_nulls.is_null_at(11), "col_date should be null");
        assert!(found_row_nulls.is_null_at(12), "col_time should be null");
        assert!(
            found_row_nulls.is_null_at(13),
            "col_timestamp should be null"
        );
        assert!(
            found_row_nulls.is_null_at(14),
            "col_timestamp_ltz should be null"
        );
        assert!(found_row_nulls.is_null_at(15), "col_bytes should be null");
        assert!(found_row_nulls.is_null_at(16), "col_binary should be null");

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }

    /// Integration test for concurrent batched lookups.
    #[tokio::test]
    async fn batched_concurrent_lookups() {
        use futures::stream::{FuturesUnordered, StreamExt};

        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path = TablePath::new("fluss".to_string(), "test_batched_lookups".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .column("value", DataTypes::bigint())
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

        // Insert only even-numbered records (0, 2, 4, ..., 98) in parallel
        let num_records = 100i32;
        let mut upsert_futures = FuturesUnordered::new();
        for i in (0..num_records).step_by(2) {
            let mut writer = table_upsert
                .create_writer()
                .expect("Failed to create writer");
            upsert_futures.push(async move {
                let mut row = GenericRow::new(3);
                row.set_field(0, i);
                row.set_field(1, format!("name_{}", i));
                row.set_field(2, (i * 100) as i64);
                writer.upsert(&row).await.expect("Failed to upsert");
            });
        }
        // Wait for all upserts to complete
        while upsert_futures.next().await.is_some() {}

        // Create multiple lookupers for concurrent lookups
        let num_lookupers = 50i32;
        let mut lookupers: Vec<_> = (0..num_lookupers)
            .map(|_| {
                table
                    .new_lookup()
                    .expect("Failed to create lookup")
                    .create_lookuper()
                    .expect("Failed to create lookuper")
            })
            .collect();

        // Run concurrent lookups
        let mut futures = FuturesUnordered::new();
        for (i, lookuper) in lookupers.iter_mut().enumerate() {
            // First 10 lookupers all lookup id=0 (same key multiple times)
            let id = if i < 10 { 0 } else { i as i32 };
            let expects_result = id % 2 == 0; // Even IDs exist

            futures.push(async move {
                let key = make_key(id);
                let result = lookuper.lookup(&key).await.expect("Failed to lookup");
                let row_opt = result.get_single_row().expect("Failed to get row");

                if expects_result {
                    let row = row_opt.unwrap_or_else(|| panic!("Row {} should exist", id));
                    assert_eq!(row.get_int(0), id, "id mismatch for key {}", id);
                    assert_eq!(
                        row.get_string(1),
                        format!("name_{}", id),
                        "name mismatch for key {}",
                        id
                    );
                    assert_eq!(
                        row.get_long(2),
                        (id * 100) as i64,
                        "value mismatch for key {}",
                        id
                    );
                } else {
                    assert!(row_opt.is_none(), "Row {} should not exist", id);
                }
                (id, expects_result)
            });
        }

        // Collect all results and verify
        let mut results = Vec::with_capacity(num_lookupers as usize);
        while let Some(result) = futures.next().await {
            results.push(result);
        }

        // Verify all lookups completed successfully
        assert_eq!(
            results.len(),
            num_lookupers as usize,
            "Not all lookups completed"
        );

        // Verify we had the expected mix of scenarios
        let same_key_lookups = results.iter().filter(|(id, _)| *id == 0).count();
        assert_eq!(same_key_lookups, 10, "Should have 10 lookups for same key");

        let non_existing_lookups = results.iter().filter(|(_, exists)| !exists).count();
        assert!(
            non_existing_lookups > 0,
            "Should have some non-existing key lookups"
        );

        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
    }
}
