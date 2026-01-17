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

use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
use crate::metadata::{
    DataField, DataTypes, Schema, TableBucket, TableDescriptor, TableInfo, TablePath,
};
use crate::rpc::{ServerConnection, ServerConnectionInner, Transport, spawn_mock_server};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::BufStream;
use tokio::task::JoinHandle;

pub(crate) async fn build_mock_connection<F>(handler: F) -> (ServerConnection, JoinHandle<()>)
where
    F: FnMut(crate::rpc::ApiKey, i32, Vec<u8>) -> Vec<u8> + Send + 'static,
{
    let (client, server) = tokio::io::duplex(1024);
    let handle = spawn_mock_server(server, handler).await;
    let transport = Transport::Test { inner: client };
    let connection = Arc::new(ServerConnectionInner::new(
        BufStream::new(transport),
        usize::MAX,
        Arc::from(""),
    ));
    (connection, handle)
}

pub(crate) fn build_table_info(table_path: TablePath, table_id: i64, buckets: i32) -> TableInfo {
    let row_type = DataTypes::row(vec![DataField::new(
        "id".to_string(),
        DataTypes::int(),
        None,
    )]);
    let mut schema_builder = Schema::builder().with_row_type(&row_type);
    let schema = schema_builder.build().expect("schema build");
    let table_descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(buckets), vec![])
        .build()
        .expect("descriptor build");
    TableInfo::of(table_path, table_id, 1, table_descriptor, 0, 0)
}

pub(crate) fn build_cluster(table_path: &TablePath, table_id: i64, buckets: i32) -> Cluster {
    let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);

    let mut servers = HashMap::new();
    servers.insert(server.id(), server.clone());

    let mut locations_by_path = HashMap::new();
    let mut locations_by_bucket = HashMap::new();
    let mut bucket_locations = Vec::new();

    for bucket_id in 0..buckets {
        let table_bucket = TableBucket::new(table_id, bucket_id);
        let bucket_location = BucketLocation::new(
            table_bucket.clone(),
            Some(server.clone()),
            table_path.clone(),
        );
        bucket_locations.push(bucket_location.clone());
        locations_by_bucket.insert(table_bucket, bucket_location);
    }
    locations_by_path.insert(table_path.clone(), bucket_locations);

    let mut table_id_by_path = HashMap::new();
    table_id_by_path.insert(table_path.clone(), table_id);

    let mut table_info_by_path = HashMap::new();
    table_info_by_path.insert(
        table_path.clone(),
        build_table_info(table_path.clone(), table_id, buckets),
    );

    Cluster::new(
        None,
        servers,
        locations_by_path,
        locations_by_bucket,
        table_id_by_path,
        table_info_by_path,
    )
}

pub(crate) fn build_cluster_arc(
    table_path: &TablePath,
    table_id: i64,
    buckets: i32,
) -> Arc<Cluster> {
    Arc::new(build_cluster(table_path, table_id, buckets))
}

pub(crate) fn build_cluster_with_coordinator(
    table_path: &TablePath,
    table_id: i64,
    coordinator: ServerNode,
    tablet: ServerNode,
) -> Cluster {
    let table_bucket = TableBucket::new(table_id, 0);
    let bucket_location = BucketLocation::new(
        table_bucket.clone(),
        Some(tablet.clone()),
        table_path.clone(),
    );

    let mut servers = HashMap::new();
    servers.insert(tablet.id(), tablet);

    let mut locations_by_path = HashMap::new();
    locations_by_path.insert(table_path.clone(), vec![bucket_location.clone()]);

    let mut locations_by_bucket = HashMap::new();
    locations_by_bucket.insert(table_bucket, bucket_location);

    let mut table_id_by_path = HashMap::new();
    table_id_by_path.insert(table_path.clone(), table_id);

    let mut table_info_by_path = HashMap::new();
    table_info_by_path.insert(
        table_path.clone(),
        build_table_info(table_path.clone(), table_id, 1),
    );

    Cluster::new(
        Some(coordinator),
        servers,
        locations_by_path,
        locations_by_bucket,
        table_id_by_path,
        table_info_by_path,
    )
}

pub(crate) fn build_cluster_with_coordinator_arc(
    table_path: &TablePath,
    table_id: i64,
    coordinator: ServerNode,
    tablet: ServerNode,
) -> Arc<Cluster> {
    Arc::new(build_cluster_with_coordinator(
        table_path,
        table_id,
        coordinator,
        tablet,
    ))
}
