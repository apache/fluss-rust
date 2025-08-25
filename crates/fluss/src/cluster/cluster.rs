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

use crate::BucketId;
use crate::cluster::{BucketLocation, ServerNode, ServerType};
use crate::error::Result;
use crate::metadata::{JsonSerde, TableBucket, TableDescriptor, TableInfo, TablePath};
use crate::proto::MetadataResponse;
use crate::rpc::{from_pb_server_node, from_pb_table_path};
use rand::random_range;
use std::collections::HashMap;

static EMPTY: Vec<BucketLocation> = Vec::new();

#[derive(Default)]
pub struct Cluster {
    coordinator_server: Option<ServerNode>,
    alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
    alive_tablet_servers: Vec<ServerNode>,
    available_locations_by_path: HashMap<TablePath, Vec<BucketLocation>>,
    available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
    table_id_by_path: HashMap<TablePath, i64>,
    table_path_by_id: HashMap<i64, TablePath>,
    table_info_by_path: HashMap<TablePath, TableInfo>,
}

impl Cluster {
    pub fn new(
        coordinator_server: Option<ServerNode>,
        alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
        available_locations_by_path: HashMap<TablePath, Vec<BucketLocation>>,
        available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
        table_id_by_path: HashMap<TablePath, i64>,
        table_info_by_path: HashMap<TablePath, TableInfo>,
    ) -> Self {
        let alive_tablet_servers = alive_tablet_servers_by_id.values().cloned().collect();
        let table_path_by_id = table_id_by_path
            .iter()
            .map(|(path, table_id)| (*table_id, path.clone()))
            .collect();
        Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
        }
    }

    pub fn update(&mut self, cluster: Cluster) {
        let Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
        } = cluster;
        self.coordinator_server = coordinator_server;
        self.alive_tablet_servers_by_id = alive_tablet_servers_by_id;
        self.alive_tablet_servers = alive_tablet_servers;
        self.available_locations_by_path = available_locations_by_path;
        self.available_locations_by_bucket = available_locations_by_bucket;
        self.table_id_by_path = table_id_by_path;
        self.table_path_by_id = table_path_by_id;
        self.table_info_by_path = table_info_by_path;
    }

    pub fn from_metadata_response(
        metadata_response: MetadataResponse,
        origin_cluster: Option<&Cluster>,
    ) -> Result<Cluster> {
        let mut servers = HashMap::with_capacity(metadata_response.tablet_servers.len());
        for pb_server in metadata_response.tablet_servers {
            let server_id = pb_server.node_id;
            let server_node = from_pb_server_node(pb_server, ServerType::TabletServer);
            servers.insert(server_id, server_node);
        }

        let coordinator_server = metadata_response
            .coordinator_server
            .map(|node| from_pb_server_node(node, ServerType::CoordinatorServer));

        let mut table_id_by_path = HashMap::new();
        let mut table_info_by_path = HashMap::new();
        if let Some(origin) = origin_cluster {
            table_info_by_path.extend(origin.get_table_info_by_path().clone());
            table_id_by_path.extend(origin.get_table_id_by_path().clone());
        }

        // Index the bucket locations by table path, and index bucket location by bucket
        let mut tmp_available_location_by_bucket = HashMap::new();
        let mut tmp_available_locations_by_path = HashMap::new();

        for table_metadata in metadata_response.table_metadata {
            let table_id = table_metadata.table_id;
            let table_path = from_pb_table_path(&table_metadata.table_path);
            let table_descriptor = TableDescriptor::deserialize_json(
                &serde_json::from_slice(table_metadata.table_json.as_slice()).unwrap(),
            )?;
            let table_info = TableInfo::of(
                table_path.clone(),
                table_id,
                table_metadata.schema_id,
                table_descriptor,
                table_metadata.created_time,
                table_metadata.modified_time,
            );
            table_info_by_path.insert(table_path.clone(), table_info);

            // now, get bucket matadata
            let mut found_unavailable_bucket = false;
            let mut available_bucket_for_table = vec![];
            let mut bucket_for_table = vec![];
            for bucket_metadata in table_metadata.bucket_metadata {
                let bucket_id = bucket_metadata.bucket_id;
                let bucket = TableBucket::new(table_id, bucket_id);
                let bucket_location;
                if let Some(leader_id) = bucket_metadata.leader_id
                    && let Some(server_node) = servers.get(&leader_id)
                {
                    bucket_location = BucketLocation::new(
                        bucket.clone(),
                        Some(server_node.clone()),
                        table_path.clone(),
                    );
                    available_bucket_for_table.push(bucket_location.clone());
                    tmp_available_location_by_bucket
                        .insert(bucket.clone(), bucket_location.clone());
                } else {
                    found_unavailable_bucket = true;
                    bucket_location = BucketLocation::new(bucket.clone(), None, table_path.clone());
                }
                bucket_for_table.push(bucket_location.clone());
            }

            if found_unavailable_bucket {
                tmp_available_locations_by_path
                    .insert(table_path.clone(), available_bucket_for_table.clone());
            } else {
                tmp_available_locations_by_path.insert(table_path.clone(), bucket_for_table);
            }
        }
        Ok(Cluster::new(
            coordinator_server,
            servers,
            tmp_available_locations_by_path,
            tmp_available_location_by_bucket,
            table_id_by_path,
            table_info_by_path,
        ))
    }

    pub fn get_coordinator_server(&self) -> Option<&ServerNode> {
        self.coordinator_server.as_ref()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        let location = self.available_locations_by_bucket.get(table_bucket);
        if let Some(location) = location {
            location.leader().as_ref()
        } else {
            None
        }
    }

    pub fn get_tablet_server(&self, id: i32) -> Option<&ServerNode> {
        self.alive_tablet_servers_by_id.get(&id)
    }

    pub fn get_table_bucket(&self, table_path: &TablePath, bucket_id: BucketId) -> TableBucket {
        let table_info = self.get_table(table_path);
        TableBucket::new(table_info.table_id, bucket_id)
    }

    pub fn get_bucket_locations_by_path(&self) -> &HashMap<TablePath, Vec<BucketLocation>> {
        &self.available_locations_by_path
    }

    pub fn get_table_info_by_path(&self) -> &HashMap<TablePath, TableInfo> {
        &self.table_info_by_path
    }

    pub fn get_table_id_by_path(&self) -> &HashMap<TablePath, i64> {
        &self.table_id_by_path
    }

    pub fn get_available_buckets_for_table_path(
        &self,
        table_path: &TablePath,
    ) -> &Vec<BucketLocation> {
        self.available_locations_by_path
            .get(table_path)
            .unwrap_or(&EMPTY)
    }

    pub fn get_one_available_server(&self) -> &ServerNode {
        assert!(
            !self.alive_tablet_servers.is_empty(),
            "no alive tablet server in cluster"
        );
        let offset = random_range(0..self.alive_tablet_servers.len());
        self.alive_tablet_servers
            .get(offset)
            .unwrap_or_else(|| panic!("can't find alive tab server by offset {offset}"))
    }

    pub fn get_bucket_count(&self, table_path: &TablePath) -> i32 {
        self.table_info_by_path
            .get(table_path)
            .unwrap_or_else(|| panic!("can't not table info by path {table_path}"))
            .num_buckets
    }

    pub fn get_table(&self, table_path: &TablePath) -> &TableInfo {
        self.table_info_by_path
            .get(table_path)
            .unwrap_or_else(|| panic!("can't find table info by path {table_path}"))
    }

    pub fn opt_get_table(&self, table_path: &TablePath) -> Option<&TableInfo> {
        self.table_info_by_path.get(table_path)
    }
}
