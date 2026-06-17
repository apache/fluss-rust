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

//! Extended admin integration tests covering APIs available on both Fluss
//! 0.9.x and 1.x servers but not exercised by `admin.rs`.
//!
//! These tests run against the shared cluster gated behind `integration_tests`.
//! Some admin operations have semantics that depend on optional cluster
//! configuration (lake storage, authorization). For those the tests assert the
//! request/response roundtrip succeeds and tolerate a *structured* server-side
//! error (a decoded [`FlussError`]) while still failing on transport/decoding
//! errors — which is what a Rust-client integration test must guard.

#[cfg(test)]
mod admin_extended_test {
    use crate::integration::utils::get_shared_cluster;
    use fluss::error::FlussError;
    use fluss::metadata::{
        DataTypes, DatabaseDescriptorBuilder, JsonSerde, KvFormat, LogFormat, Schema,
        TableDescriptor, TablePath,
    };
    use fluss::proto;

    // ACL enum codes (mirrors the Java security model).
    const RESOURCE_TYPE_DATABASE: i32 = 3;
    const OPERATION_TYPE_READ: i32 = 3;
    const PERMISSION_TYPE_ALLOW: i32 = 2;

    // AlterConfigOpType codes.
    const OP_TYPE_SET: i32 = 0;

    // ServerTag codes.
    const SERVER_TAG_TEMPORARY_OFFLINE: i32 = 1;

    /// Asserts an error decoded into one of the `allowed` Fluss API errors.
    /// Panics (failing the test) on a transport/decoding error or an unexpected
    /// API error code.
    fn assert_expected_api_error(error: fluss::error::Error, allowed: &[FlussError]) {
        match error.api_error() {
            Some(code) if allowed.contains(&code) => {}
            other => panic!(
                "Expected one of {allowed:?} but got {other:?} (full error: {error:?})"
            ),
        }
    }

    /// Builds a simple non-partitioned log table descriptor (id INT, name STRING).
    fn simple_log_table() -> TableDescriptor {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .expect("build schema");
        TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .build()
            .expect("build table descriptor")
    }

    /// Builds a simple primary-key/KV table descriptor (id INT PK, name STRING).
    fn simple_kv_table() -> TableDescriptor {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("build schema");
        TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["id".to_string()])
            .property("table.replication.factor", "1")
            .log_format(LogFormat::ARROW)
            .kv_format(KvFormat::COMPACTED)
            .build()
            .expect("build table descriptor")
    }

    // ---------------------------------------------------------------------
    // Group A: Database listing & summaries
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_databases() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_a = "test_list_databases_a";
        let db_b = "test_list_databases_b";
        let descriptor = DatabaseDescriptorBuilder::default().build();

        admin
            .create_database(db_a, Some(&descriptor), true)
            .await
            .unwrap();
        admin
            .create_database(db_b, Some(&descriptor), true)
            .await
            .unwrap();

        let databases = admin.list_databases().await.expect("should list databases");
        assert!(
            databases.iter().any(|d| d == db_a),
            "Expected {db_a} in {databases:?}"
        );
        assert!(
            databases.iter().any(|d| d == db_b),
            "Expected {db_b} in {databases:?}"
        );

        admin.drop_database(db_a, true, true).await.unwrap();
        admin.drop_database(db_b, true, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_database_summaries() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_list_db_summaries";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_descriptor = simple_log_table();
        for table in ["summary_t1", "summary_t2"] {
            admin
                .create_table(&TablePath::new(db_name, table), &table_descriptor, true)
                .await
                .unwrap();
        }

        let summaries = admin
            .list_database_summaries()
            .await
            .expect("should list database summaries");

        let summary = summaries
            .iter()
            .find(|s| s.database_name == db_name)
            .unwrap_or_else(|| panic!("Expected summary for {db_name} in {summaries:?}"));

        assert_eq!(
            summary.table_count, 2,
            "Database {db_name} should report 2 tables"
        );
        assert!(
            summary.created_time > 0,
            "created_time should be positive, got {}",
            summary.created_time
        );

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group B: Schema operations
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_table_schema() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_get_table_schema_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "schema_table");
        let table_descriptor = simple_kv_table();
        admin
            .create_table(&table_path, &table_descriptor, true)
            .await
            .unwrap();

        // Request the latest schema (schema_id = None).
        let schema_info = admin
            .get_table_schema(&table_path, None)
            .await
            .expect("should get latest table schema");
        assert!(
            schema_info.schema_id() > 0,
            "schema_id should be positive, got {}",
            schema_info.schema_id()
        );
        assert_eq!(
            schema_info.schema().columns().len(),
            2,
            "schema should have 2 columns"
        );

        // Request the same schema by explicit id.
        let by_id = admin
            .get_table_schema(&table_path, Some(schema_info.schema_id()))
            .await
            .expect("should get table schema by id");
        assert_eq!(by_id.schema_id(), schema_info.schema_id());
        assert_eq!(by_id.schema().columns().len(), 2);

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group C: Alter operations
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_alter_table_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "alter_table");
        admin
            .create_table(&table_path, &simple_log_table(), true)
            .await
            .unwrap();

        // Add a nullable "email" column at the end of the schema.
        let data_type_json = serde_json::to_vec(
            &DataTypes::string()
                .serialize_json()
                .expect("serialize STRING type"),
        )
        .expect("encode data_type_json");
        let add_column = proto::PbAddColumn {
            column_name: "email".to_string(),
            data_type_json,
            comment: Some("user email".to_string()),
            column_position_type: 0, // LAST (the only position the server supports)
        };

        admin
            .alter_table(&table_path, vec![], vec![add_column])
            .await
            .expect("should add column");

        let schema_info = admin
            .get_table_schema(&table_path, None)
            .await
            .expect("should get schema after alter");
        assert_eq!(
            schema_info.schema().columns().len(),
            3,
            "schema should have 3 columns after adding email"
        );
        assert!(
            schema_info
                .schema()
                .columns()
                .iter()
                .any(|c| c.name() == "email"),
            "schema should contain the new 'email' column"
        );

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_alter_database() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_alter_database_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let config_change = proto::PbAlterConfig {
            config_key: "custom.key".to_string(),
            config_value: Some("custom-value".to_string()),
            op_type: OP_TYPE_SET,
        };
        // AlterDatabase is not implemented on every server build (e.g. 0.9.x).
        // Accept the server's "unsupported" signal but never a transport failure.
        match admin
            .alter_database(db_name, vec![config_change], false)
            .await
        {
            Ok(()) => {
                // Altering a non-existent database with ignore_if_not_exists = true is a no-op.
                admin
                    .alter_database("no_such_db_for_alter", vec![], true)
                    .await
                    .expect("altering missing db with ignore flag should succeed");
            }
            Err(fluss::error::Error::UnsupportedVersion { .. }) => {}
            Err(error) => panic!("unexpected error from alter_database: {error:?}"),
        }

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group D: Cluster configuration
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_describe_cluster_configs() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let response = admin
            .describe_cluster_configs()
            .await
            .expect("should describe cluster configs");

        for config in &response.configs {
            assert!(
                !config.config_key.is_empty(),
                "config_key should not be empty"
            );
            assert!(
                !config.config_source.is_empty(),
                "config_source should not be empty for {}",
                config.config_key
            );
        }
    }

    #[tokio::test]
    async fn test_alter_cluster_configs() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        // Read an existing config that has a value, then SET it to the same value.
        // This exercises the write path without changing cluster behaviour.
        let described = admin
            .describe_cluster_configs()
            .await
            .expect("should describe cluster configs");

        let Some(existing) = described
            .configs
            .iter()
            .find(|c| c.config_value.is_some())
        else {
            // No config with a value to round-trip; nothing to assert.
            return;
        };

        let alter = proto::PbAlterConfig {
            config_key: existing.config_key.clone(),
            config_value: existing.config_value.clone(),
            op_type: OP_TYPE_SET,
        };

        // Many keys are not dynamically alterable; the server rejects those with
        // either InvalidConfigException or a generic "not allowed to be changed
        // dynamically" error. Either way the request/response roundtrip worked.
        if let Err(error) = admin.alter_cluster_configs(vec![alter]).await {
            assert_expected_api_error(
                error,
                &[
                    FlussError::InvalidConfigException,
                    FlussError::UnknownServerError,
                ],
            );
        }
    }

    // ---------------------------------------------------------------------
    // Group E: Table statistics
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_table_stats() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_get_table_stats_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "stats_table");
        admin
            .create_table(&table_path, &simple_kv_table(), true)
            .await
            .unwrap();
        let table_id = admin
            .get_table_info(&table_path)
            .await
            .unwrap()
            .get_table_id();

        let buckets_req = vec![proto::PbTableStatsReqForBucket {
            partition_id: None,
            bucket_id: 0,
        }];
        // GetTableStats is not implemented on every server build. Accept either a
        // decoded response or the server's "unsupported" signal, but never a
        // transport/decoding failure.
        match admin.get_table_stats(table_id, buckets_req).await {
            Ok(response) => {
                for bucket in &response.buckets_resp {
                    // Per-bucket entries echo the requested bucket id.
                    assert_eq!(bucket.bucket_id, 0, "unexpected bucket id in stats response");
                }
            }
            Err(fluss::error::Error::UnsupportedVersion { .. }) => {}
            Err(error) => panic!("unexpected error from get_table_stats: {error:?}"),
        }

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group F: KV snapshot operations (0.9.x)
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_latest_kv_snapshots() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_latest_kv_snapshots_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "latest_kv_snapshots_table");
        admin
            .create_table(&table_path, &simple_kv_table(), true)
            .await
            .unwrap();
        let table_id = admin
            .get_table_info(&table_path)
            .await
            .unwrap()
            .get_table_id();

        let response = admin
            .get_latest_kv_snapshots(&table_path, None)
            .await
            .expect("should get latest kv snapshots");
        assert_eq!(
            response.table_id, table_id,
            "response table_id should match the requested table"
        );

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_kv_snapshot_lease_lifecycle() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_kv_snapshot_lease_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "lease_table");
        admin
            .create_table(&table_path, &simple_kv_table(), true)
            .await
            .unwrap();
        let table_id = admin
            .get_table_info(&table_path)
            .await
            .unwrap()
            .get_table_id();

        // A fresh table has no snapshot to lease, so the requested snapshot is
        // reported back as unavailable. The RPC itself must still succeed.
        let lease = proto::PbKvSnapshotLeaseForTable {
            table_id,
            bucket_snapshots: vec![proto::PbKvSnapshotLeaseForBucket {
                partition_id: None,
                bucket_id: 0,
                snapshot_id: 0,
            }],
        };
        let response = admin
            .create_kv_snapshot_lease("test-lease-id", 60_000, vec![lease])
            .await
            .expect("should acquire kv snapshot lease");

        // The fresh-table snapshot is unavailable; just confirm the response decodes.
        let _ = response.unavailable_snapshots;

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group G: Server tags
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_server_tag_lifecycle() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let nodes = admin.get_server_nodes().await.expect("should get nodes");
        let tablet_id = nodes
            .iter()
            .find(|n| *n.server_type() == fluss::ServerType::TabletServer)
            .map(|n| n.id())
            .expect("expected a tablet server node");

        // Add then immediately remove a TEMPORARY_OFFLINE tag so cluster state
        // is restored. Both RPCs must complete without a transport error.
        admin
            .add_server_tag(vec![tablet_id], SERVER_TAG_TEMPORARY_OFFLINE)
            .await
            .expect("should add server tag");
        admin
            .remove_server_tag(vec![tablet_id], SERVER_TAG_TEMPORARY_OFFLINE)
            .await
            .expect("should remove server tag");
    }

    // ---------------------------------------------------------------------
    // Group H: Rebalance
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_rebalance_operations() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        // No rebalance is running; listing progress and cancelling are read/no-op
        // paths. Tolerate a structured server error (e.g. nothing to cancel).
        if let Err(error) = admin.list_rebalance_progress(None).await {
            assert_expected_api_error(
                error,
                &[
                    FlussError::UnknownServerError,
                    FlussError::InvalidCoordinatorException,
                ],
            );
        }

        if let Err(error) = admin.cancel_rebalance(None).await {
            assert_expected_api_error(
                error,
                &[
                    FlussError::UnknownServerError,
                    FlussError::InvalidCoordinatorException,
                ],
            );
        }
    }

    // ---------------------------------------------------------------------
    // Group I: Producer offsets
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_producer_offsets_lifecycle() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_producer_offsets_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "producer_offsets_table");
        admin
            .create_table(&table_path, &simple_log_table(), true)
            .await
            .unwrap();
        let table_id = admin
            .get_table_info(&table_path)
            .await
            .unwrap()
            .get_table_id();

        let producer_id = "test-producer";
        let table_offsets = vec![proto::PbProducerTableOffsets {
            table_id,
            bucket_offsets: vec![proto::PbBucketOffset {
                partition_id: None,
                bucket_id: 0,
                log_end_offset: Some(42),
            }],
        }];

        admin
            .register_producer_offsets(producer_id, table_offsets)
            .await
            .expect("should register producer offsets");

        let fetched = admin
            .get_producer_offsets(producer_id)
            .await
            .expect("should get producer offsets");
        let registered = fetched
            .table_offsets
            .iter()
            .find(|t| t.table_id == table_id)
            .unwrap_or_else(|| panic!("expected offsets for table {table_id}"));
        assert_eq!(
            registered.bucket_offsets.first().and_then(|b| b.log_end_offset),
            Some(42),
            "registered log_end_offset should be 42"
        );

        admin
            .delete_producer_offsets(producer_id)
            .await
            .expect("should delete producer offsets");

        let after_delete = admin
            .get_producer_offsets(producer_id)
            .await
            .expect("should get producer offsets after delete");
        assert!(
            after_delete
                .table_offsets
                .iter()
                .all(|t| t.table_id != table_id),
            "offsets for table {table_id} should be gone after delete"
        );

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group J: ACL management
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_acl_lifecycle() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_acl_lifecycle_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let acl = proto::PbAclInfo {
            resource_name: db_name.to_string(),
            resource_type: RESOURCE_TYPE_DATABASE,
            principal_name: "alice".to_string(),
            principal_type: "User".to_string(),
            host: "*".to_string(),
            operation_type: OPERATION_TYPE_READ,
            permission_type: PERMISSION_TYPE_ALLOW,
        };

        // Authorization may be disabled on the shared cluster. In that case the
        // server rejects the request with SecurityDisabledException; otherwise
        // the full create/list/drop lifecycle should round-trip.
        match admin.create_acls(vec![acl.clone()]).await {
            Err(error) => {
                assert_expected_api_error(
                    error,
                    &[
                        FlussError::SecurityDisabledException,
                        FlussError::AuthorizationException,
                    ],
                );
            }
            Ok(_) => {
                let filter = proto::PbAclFilter {
                    resource_name: Some(db_name.to_string()),
                    resource_type: RESOURCE_TYPE_DATABASE,
                    principal_name: Some("alice".to_string()),
                    principal_type: Some("User".to_string()),
                    host: Some("*".to_string()),
                    operation_type: OPERATION_TYPE_READ,
                    permission_type: PERMISSION_TYPE_ALLOW,
                };
                let listed = admin
                    .list_acls(filter.clone())
                    .await
                    .expect("should list acls");
                assert!(
                    listed.acl.iter().any(|a| a.resource_name == db_name),
                    "created ACL should appear in list: {listed:?}"
                );

                admin
                    .drop_acls(vec![filter])
                    .await
                    .expect("should drop acls");
            }
        }

        admin.drop_database(db_name, true, true).await.unwrap();
    }

    // ---------------------------------------------------------------------
    // Group K: Lake snapshots (0.9.x)
    // ---------------------------------------------------------------------

    #[tokio::test]
    async fn test_lake_snapshot_operations() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("should get admin");

        let db_name = "test_lake_snapshot_db";
        let descriptor = DatabaseDescriptorBuilder::default().build();
        admin
            .create_database(db_name, Some(&descriptor), true)
            .await
            .unwrap();

        let table_path = TablePath::new(db_name, "lake_table");
        admin
            .create_table(&table_path, &simple_log_table(), true)
            .await
            .unwrap();

        // Lake storage is typically not configured for the test cluster, so both
        // calls are expected to fail with a lake-related API error rather than a
        // transport error.
        let lake_errors = [
            FlussError::LakeStorageNotConfiguredException,
            FlussError::LakeSnapshotNotExist,
        ];

        if let Err(error) = admin.get_latest_lake_snapshot(&table_path).await {
            assert_expected_api_error(error, &lake_errors);
        }
        if let Err(error) = admin.get_lake_snapshot(&table_path, None).await {
            assert_expected_api_error(error, &lake_errors);
        }

        admin.drop_database(db_name, true, true).await.unwrap();
    }
}
