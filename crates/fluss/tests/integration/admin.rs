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
    use fluss::metadata::DatabaseDescriptorBuilder;
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
        // todo
    }
}
