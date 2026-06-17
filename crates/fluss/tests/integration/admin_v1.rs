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

//! Integration tests for admin APIs that are only available on Fluss 1.x
//! servers (API keys 1057-1058, 1061-1064). The whole module is gated behind
//! the `fluss_v1` feature so it is skipped when running against a 0.9.x server.

#[cfg(test)]
mod admin_v1_test {
    use crate::integration::utils::get_shared_cluster;
    use fluss::metadata::{
        DatabaseDescriptorBuilder, KvFormat, LogFormat, Schema, TableDescriptor, TablePath,
    };
    use fluss::metadata::DataTypes;

    /// `get_cluster_health` (API key 1062) reports replica/leader counts and a
    /// status code for the cluster.
    #[tokio::test]
    async fn test_get_cluster_health() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let response = admin
            .get_cluster_health()
            .await
            .expect("should get cluster health");

        assert!(
            response.status >= 0,
            "Cluster health status should be non-negative, got: {}",
            response.status
        );
    }

    /// `list_kv_snapshots` (API key 1064) returns the active snapshots for a KV
    /// table. A freshly created table has none, but the response must echo the
    /// requested table id.
    #[tokio::test]
    async fn test_list_kv_snapshots() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let test_db_name = "test_list_kv_snapshots_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_list_kv_snapshots")
            .build();

        admin
            .create_database(test_db_name, Some(&db_descriptor), true)
            .await
            .expect("Failed to create test database");

        let test_table_name = "kv_snapshot_table";
        let table_path = TablePath::new(test_db_name, test_table_name);

        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("Failed to build table schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema)
            .distributed_by(Some(1), vec!["id".to_string()])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::COMPACTED)
            .build()
            .expect("Failed to build table descriptor");

        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .expect("Failed to create table");

        let table_info = admin
            .get_table_info(&table_path)
            .await
            .expect("should get table info");
        let table_id = table_info.get_table_id();

        let response = admin
            .list_kv_snapshots(table_id, None)
            .await
            .expect("should list kv snapshots");

        assert_eq!(
            response.table_id, table_id,
            "Response table_id should match request"
        );

        // Cleanup
        admin.drop_table(&table_path, true).await.unwrap();
        admin
            .drop_database(test_db_name, true, true)
            .await
            .unwrap();
    }

    /// `list_remote_log_manifests` (API key 1063) lists tiered log segments. A
    /// newly created table has had no remote log activity yet.
    #[tokio::test]
    async fn test_list_remote_log_manifests() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let test_db_name = "test_list_remote_log_manifests_db";
        let db_descriptor = DatabaseDescriptorBuilder::default()
            .comment("Database for test_list_remote_log_manifests")
            .build();

        admin
            .create_database(test_db_name, Some(&db_descriptor), true)
            .await
            .expect("Failed to create test database");

        let test_table_name = "remote_log_table";
        let table_path = TablePath::new(test_db_name, test_table_name);

        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("data", DataTypes::string())
            .build()
            .expect("Failed to build table schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema)
            .distributed_by(Some(1), vec![])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .build()
            .expect("Failed to build table descriptor");

        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .expect("Failed to create table");

        let table_info = admin
            .get_table_info(&table_path)
            .await
            .expect("should get table info");
        let table_id = table_info.get_table_id();

        let response = admin
            .list_remote_log_manifests(table_id, None)
            .await
            .expect("should list remote log manifests");

        // A newly created table with no remote log activity should return empty manifests.
        assert!(
            response.manifests.is_empty(),
            "Newly created table should have no remote log manifests"
        );

        // Cleanup
        admin.drop_table(&table_path, true).await.unwrap();
        admin
            .drop_database(test_db_name, true, true)
            .await
            .unwrap();
    }

    /// `drop_kv_snapshot_lease` (API key 1058) removes an entire lease. Dropping
    /// a lease that never existed is a server-side no-op.
    #[tokio::test]
    async fn test_drop_kv_snapshot_lease() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        // Dropping a non-existent lease should succeed (no-op on server)
        let result = admin.drop_kv_snapshot_lease("non-existent-lease-id").await;
        assert!(
            result.is_ok(),
            "Dropping non-existent lease should succeed, got: {:?}",
            result
        );
    }

    /// `release_kv_snapshot_lease` (API key 1057) releases specific buckets from
    /// a lease. Releasing an empty bucket set against an unknown lease is a
    /// no-op and must not error.
    #[tokio::test]
    async fn test_release_kv_snapshot_lease() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let result = admin
            .release_kv_snapshot_lease("non-existent-lease-id", vec![])
            .await;
        assert!(
            result.is_ok(),
            "Releasing an empty bucket set should succeed, got: {:?}",
            result
        );
    }
}
