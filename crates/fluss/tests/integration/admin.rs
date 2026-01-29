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
use parking_lot::RwLock;

use std::sync::Arc;
use std::sync::LazyLock;

#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: LazyLock<Arc<RwLock<Option<FlussTestingCluster>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod admin_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::FlussTestingCluster;
    use crate::integration::utils::{get_cluster, start_cluster, stop_cluster};
    use fluss::error::FlussError;
    use fluss::metadata::{
        DataTypes, DatabaseDescriptorBuilder, KvFormat, LogFormat, PartitionSpec, Schema,
        TableDescriptor, TablePath,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn before_all() {
        start_cluster("test-admin", SHARED_FLUSS_CLUSTER.clone());
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        get_cluster(&SHARED_FLUSS_CLUSTER)
    }

    fn after_all() {
        stop_cluster(SHARED_FLUSS_CLUSTER.clone());
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
        admin
            .drop_database(db_name, false, true)
            .await
            .expect("should drop_database");

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
            .primary_key(vec!["id".to_string()])
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
        assert_eq!(
            tables.len(),
            1,
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
        let actual_schema = table_info.get_schema();
        assert_eq!(actual_schema, table_descriptor.schema(), "Schema mismatch");

        // verify primary key
        assert_eq!(
            table_info.get_primary_keys(),
            &vec!["id".to_string()],
            "Primary key columns mismatch"
        );

        // verify distribution and properties
        assert_eq!(table_info.get_num_buckets(), 3, "Bucket count mismatch");
        assert_eq!(
            table_info.get_bucket_keys(),
            &vec!["id".to_string()],
            "Bucket keys mismatch"
        );

        assert_eq!(
            table_info.get_properties(),
            table_descriptor.properties(),
            "Properties mismatch"
        );

        // drop table
        admin
            .drop_table(&table_path, false)
            .await
            .expect("Failed to drop table");
        // table shouldn't exist now
        assert_eq!(admin.table_exists(&table_path).await.unwrap(), false);

        // drop database
        admin
            .drop_database(test_db_name, false, true)
            .await
            .expect("Should drop database");

        // database shouldn't exist now
        assert_eq!(admin.database_exists(test_db_name).await.unwrap(), false);
    }

    #[tokio::test]
    async fn test_partition_apis() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .await
            .expect("Failed to get admin client");

        let test_db_name = "test_partition_apis_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_partition_apis")
            .build();

        admin
            .create_database(test_db_name, true, Some(&db_descriptor))
            .await
            .expect("Failed to create test database");

        let test_table_name = "partitioned_table";
        let table_path = TablePath::new(test_db_name.to_string(), test_table_name.to_string());

        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("dt", DataTypes::string())
            .column("region", DataTypes::string())
            .primary_key(vec![
                "id".to_string(),
                "dt".to_string(),
                "region".to_string(),
            ])
            .build()
            .expect("Failed to build table schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema)
            .distributed_by(Some(3), vec!["id".to_string()])
            .partitioned_by(vec!["dt".to_string(), "region".to_string()])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::COMPACTED)
            .build()
            .expect("Failed to build table descriptor");

        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .expect("Failed to create partitioned table");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert!(
            partitions.is_empty(),
            "Expected no partitions initially, found {}",
            partitions.len()
        );

        let mut partition_values = HashMap::new();
        partition_values.insert("dt".to_string(), "2024-01-15".to_string());
        partition_values.insert("region".to_string(), "EMEA".to_string());
        let partition_spec = PartitionSpec::new(partition_values);

        admin
            .create_partition(&table_path, &partition_spec, false)
            .await
            .expect("Failed to create partition");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert_eq!(
            partitions.len(),
            1,
            "Expected exactly one partition after creation"
        );
        assert_eq!(
            partitions[0].get_partition_name(),
            "2024-01-15$EMEA",
            "Partition name mismatch"
        );

        // list with partial spec filter - should find the partition
        let mut partition_values = HashMap::new();
        partition_values.insert("dt".to_string(), "2024-01-15".to_string());
        let partial_partition_spec = PartitionSpec::new(partition_values);

        let partitions_with_spec = admin
            .list_partition_infos_with_spec(&table_path, Some(&partial_partition_spec))
            .await
            .expect("Failed to list partitions with spec");
        assert_eq!(
            partitions_with_spec.len(),
            1,
            "Expected one partition matching the spec"
        );
        assert_eq!(
            partitions_with_spec[0].get_partition_name(),
            "2024-01-15$EMEA",
            "Partition name mismatch with spec filter"
        );

        // list with non-matching spec - should find no partitions
        let mut non_matching_values = HashMap::new();
        non_matching_values.insert("dt".to_string(), "2024-01-16".to_string());
        let non_matching_spec = PartitionSpec::new(non_matching_values);
        let partitions_non_matching = admin
            .list_partition_infos_with_spec(&table_path, Some(&non_matching_spec))
            .await
            .expect("Failed to list partitions with non-matching spec");
        assert!(
            partitions_non_matching.is_empty(),
            "Expected no partitions for non-matching spec"
        );

        admin
            .drop_partition(&table_path, &partition_spec, false)
            .await
            .expect("Failed to drop partition");

        let partitions = admin
            .list_partition_infos(&table_path)
            .await
            .expect("Failed to list partitions");
        assert!(
            partitions.is_empty(),
            "Expected no partitions after drop, found {}",
            partitions.len()
        );

        admin
            .drop_table(&table_path, true)
            .await
            .expect("Failed to drop table");
        admin
            .drop_database(test_db_name, true, true)
            .await
            .expect("Should drop database");
    }

    #[tokio::test]
    async fn test_fluss_error_response() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .await
            .expect("Failed to get admin client");

        let table_path = TablePath::new("fluss".to_string(), "not_exist".to_string());

        let result = admin.get_table(&table_path).await;
        assert!(result.is_err(), "Expected error but got Ok");

        let error = result.unwrap_err();
        match error {
            fluss::error::Error::FlussAPIError { api_error } => {
                assert_eq!(
                    api_error.code,
                    FlussError::TableNotExist.code(),
                    "Expected error code 7 (TableNotExist)"
                );
                assert_eq!(
                    api_error.message, "Table 'fluss.not_exist' does not exist.",
                    "Expected specific error message"
                );
            }
            other => panic!("Expected FlussAPIError, got {:?}", other),
        }
    }
}
