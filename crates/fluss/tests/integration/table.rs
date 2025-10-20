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
mod table_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::create_table;
    use arrow::array::record_batch;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    fn before_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let cluster = FlussTestingClusterBuilder::new("test_table").build().await;
                let mut guard = cluster_guard.write();
                *guard = Some(cluster);
            });
        })
        .join()
        .expect("Failed to create cluster");
        // wait for 10 seconds to avoid the error like
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
    async fn append_record_batch() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;

        let admin = connection.get_admin().await.expect("Failed to get admin");

        let table_path =
            TablePath::new("fluss".to_string(), "test_append_record_batch".to_string());

        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("c1", DataTypes::int())
                    .column("c2", DataTypes::string())
                    .build()
                    .expect("Failed to build schema"),
            )
            .build()
            .expect("Failed to build table");

        create_table(&admin, &table_path, &table_descriptor).await;

        let append_writer = connection
            .get_table(&table_path)
            .await
            .expect("Failed to get table")
            .new_append()
            .expect("Failed to create append")
            .create_writer();

        let batch1 =
            record_batch!(("c1", Int32, [1, 2, 3]), ("c2", Utf8, ["a1", "a2", "a3"])).unwrap();
        append_writer
            .append_arrow_batch(batch1)
            .await
            .expect("Failed to append batch");

        let batch2 =
            record_batch!(("c1", Int32, [4, 5, 6]), ("c2", Utf8, ["a4", "a5", "a6"])).unwrap();
        append_writer
            .append_arrow_batch(batch2)
            .await
            .expect("Failed to append batch");

        // todo: add scan code to verify the records appended in #30
    }
}
