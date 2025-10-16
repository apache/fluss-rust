// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::integration::fluss_cluster::FlussTestingCluster;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::Arc;

#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: Lazy<Arc<RwLock<Option<FlussTestingCluster>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod admin_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use fluss::metadata::{
        DataTypes, DatabaseDescriptorBuilder, KvFormat, LogFormat, Schema, TableDescriptor,
        TablePath,
    };
    use std::sync::Arc;

    fn before_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let cluster = FlussTestingClusterBuilder::new().build().await;
                let mut guard = cluster_guard.write();
                *guard = Some(cluster);
            });
        })
        .join()
        .expect("Failed to create cluster");
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

    #[tokio::test]
    async fn test_create_database() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("should get admin");

        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("test_db")
            .custom_properties(
                [
                    ("k1".to_string(), "v1".to_string()),
                    ("k2".to_string(), "v2".to_string()),
                ]
                .into(),
            )
            .build();

        let db_name = "test_create_database";

        assert_eq!(admin.database_exists(db_name).await.unwrap(), false);

        // create database
        admin
            .create_database(db_name, false, Some(&db_descriptor))
            .await
            .expect("should create database");

        // database should exist
        assert_eq!(admin.database_exists(db_name).await.unwrap(), true);

        // get database
        let db_info = admin
            .get_database_info(db_name)
            .await
            .expect("should get database info");

        assert_eq!(db_info.database_name(), db_name);
        assert_eq!(db_info.database_descriptor(), &db_descriptor);

        // drop database
        admin.drop_database(db_name, false, true).await;

        // database shouldn't exist now
        assert_eq!(admin.database_exists(db_name).await.unwrap(), false);

        // Note: We don't stop the shared cluster here as it's used by other tests
    }

    #[tokio::test]
    async fn test_create_table() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .await
            .expect("Failed to get admin client");

        let test_db_name = "test_create_table_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_create_table")
            .build();

        assert_eq!(admin.database_exists(test_db_name).await.unwrap(), false);
        admin
            .create_database(test_db_name, false, Some(&db_descriptor))
            .await
            .expect("Failed to create test database");

        let test_table_name = "test_user_table";
        let table_path = TablePath::new(test_db_name.to_string(), test_table_name.to_string());

        // build table schema
        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("age", DataTypes::int())
            .with_comment("User's age (optional)")
            .column("email", DataTypes::string())
            .primary_key_named("PK_test_user_table", vec!["id".to_string()])
            .build()
            .expect("Failed to build table schema");

        // build table descriptor
        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema.clone())
            .comment("Test table for user data (id, name, age, email)")
            .distributed_by(Some(3), vec!["id".to_string()])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::INDEXED)
            .build()
            .expect("Failed to build table descriptor");

        // create test table
        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .expect("Failed to create test table");

        assert!(
            admin.table_exists(&table_path).await.unwrap(),
            "Table {:?} should exist after creation",
            table_path
        );

        let tables = admin.list_tables(test_db_name).await.unwrap();
        assert!(
            tables.len() == 1,
            "There should be exactly one table in the database"
        );
        assert!(
            tables.contains(&test_table_name.to_string()),
            "Table list should contain the created table"
        );

        let table_info = admin
            .get_table(&table_path)
            .await
            .expect("Failed to get table info");

        // verify table comment
        assert_eq!(
            table_info.get_comment(),
            Some("Test table for user data (id, name, age, email)"),
            "Table comment mismatch"
        );

        // verify schema columns
        let actual_columns = table_info.get_schema().columns();
        let expected_columns = vec![
            ("id", DataTypes::int().as_non_nullable(), None),
            ("name", DataTypes::string(), None),
            ("age", DataTypes::int(), Some("User's age (optional)")),
            ("email", DataTypes::string(), None),
        ];

        assert_eq!(
            actual_columns.len(),
            expected_columns.len(),
            "Number of columns mismatch"
        );
        for (idx, (exp_name, exp_type, exp_comment)) in expected_columns.into_iter().enumerate() {
            let actual_col = &actual_columns[idx];
            assert_eq!(
                actual_col.name(),
                exp_name,
                "Column name mismatch at index {}",
                idx
            );
            assert_eq!(
                actual_col.data_type(),
                &exp_type,
                "Column type mismatch for {}",
                exp_name
            );
            assert_eq!(
                actual_col.comment(),
                exp_comment,
                "Column comment mismatch for {}",
                exp_name
            );
        }

        // verify primary key
        assert_eq!(
            table_info.get_primary_keys(),
            &vec!["id".to_string()],
            "Primary key columns mismatch"
        );
        assert_eq!(
            table_info
                .get_schema()
                .primary_key()
                .unwrap()
                .constraint_name(),
            "PK_id",
            "Primary key constraint name mismatch"
        );

        // verify distribution and properties
        assert_eq!(table_info.get_num_buckets(), 3, "Bucket count mismatch");
        assert_eq!(
            table_info.get_bucket_keys(),
            &vec!["id".to_string()],
            "Bucket keys mismatch"
        );
        assert_eq!(
            table_info.get_properties().get("table.replication.factor"),
            Some(&"1".to_string()),
            "Replication factor mismatch"
        );
        assert_eq!(
            LogFormat::parse(table_info.get_properties().get("table.log.format").unwrap()).unwrap(),
            LogFormat::ARROW,
            "Log format mismatch"
        );
        assert_eq!(
            KvFormat::parse(table_info.get_properties().get("table.kv.format").unwrap()).unwrap(),
            KvFormat::INDEXED,
            "KV format mismatch"
        );

        // drop table
        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
        // table shouldn't exist now
        assert_eq!(admin.table_exists(&table_path).await.unwrap(), false);

        // drop database
        admin.drop_database(test_db_name, false, true).await;

        // database shouldn't exist now
        assert_eq!(admin.database_exists(test_db_name).await.unwrap(), false);
    }
}
