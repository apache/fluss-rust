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

use crate::proto::PbTableStatsReqForBucket;

/// Per-bucket request item for `GetTableStats`.
/// Mirrors the bucket-stats request shape used by the Java client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketStatsRequest {
    pub partition_id: Option<i64>,
    pub bucket_id: i32,
}

impl BucketStatsRequest {
    pub fn new(partition_id: Option<i64>, bucket_id: i32) -> Self {
        Self {
            partition_id,
            bucket_id,
        }
    }

    pub fn to_pb(&self) -> PbTableStatsReqForBucket {
        PbTableStatsReqForBucket {
            partition_id: self.partition_id,
            bucket_id: self.bucket_id,
        }
    }

    pub fn from_pb(pb: &PbTableStatsReqForBucket) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_stats_request_pb_roundtrip() {
        for req in [
            BucketStatsRequest::new(None, 0),
            BucketStatsRequest::new(Some(42), 7),
        ] {
            let pb = req.to_pb();
            assert_eq!(BucketStatsRequest::from_pb(&pb), req);
        }
    }
}
