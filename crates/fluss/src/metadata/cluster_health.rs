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

use crate::proto::GetClusterHealthResponse;

/// Result of `get_cluster_health`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealth {
    pub num_replicas: i32,
    pub in_sync_replicas: i32,
    pub num_leader_replicas: i32,
    pub active_leader_replicas: i32,
    pub status: i32,
}

impl ClusterHealth {
    pub fn from_pb(pb: &GetClusterHealthResponse) -> Self {
        Self {
            num_replicas: pb.num_replicas,
            in_sync_replicas: pb.in_sync_replicas,
            num_leader_replicas: pb.num_leader_replicas,
            active_leader_replicas: pb.active_leader_replicas,
            status: pb.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_health_from_pb() {
        let pb = GetClusterHealthResponse {
            num_replicas: 5,
            in_sync_replicas: 4,
            num_leader_replicas: 3,
            active_leader_replicas: 3,
            status: 1,
        };
        let h = ClusterHealth::from_pb(&pb);
        assert_eq!(h.num_replicas, 5);
        assert_eq!(h.status, 1);
    }
}
