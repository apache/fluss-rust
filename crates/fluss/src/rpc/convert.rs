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

use crate::cluster::{ServerNode, ServerType};
use crate::metadata::TablePath;
use crate::proto::{PbServerNode, PbTablePath};

pub fn to_table_path(table_path: &TablePath) -> PbTablePath {
    PbTablePath {
        database_name: table_path.database().to_string(),
        table_name: table_path.table().to_string(),
    }
}

pub fn from_pb_server_node(pb_server_node: PbServerNode, server_type: ServerType) -> ServerNode {
    ServerNode::new(
        pb_server_node.node_id,
        pb_server_node.host,
        pb_server_node.port as u32,
        server_type,
    )
}

pub fn from_pb_table_path(pb_table_path: &PbTablePath) -> TablePath {
    TablePath::new(
        pb_table_path.database_name.to_string(),
        pb_table_path.table_name.to_string(),
    )
}
