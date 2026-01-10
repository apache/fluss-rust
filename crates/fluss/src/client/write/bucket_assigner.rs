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

use crate::bucketing::BucketingFunction;
use crate::cluster::Cluster;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::TablePath;
use rand::Rng;
use std::sync::atomic::{AtomicI32, Ordering};

pub trait BucketAssigner: Sync + Send {
    fn abort_if_batch_full(&self) -> bool;

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32);

    fn assign_bucket(&self, bucket_key: Option<&[u8]>, cluster: &Cluster) -> Result<i32>;
}

#[derive(Debug)]
pub struct StickyBucketAssigner {
    table_path: TablePath,
    current_bucket_id: AtomicI32,
}

impl StickyBucketAssigner {
    pub fn new(table_path: TablePath) -> Self {
        Self {
            table_path,
            current_bucket_id: AtomicI32::new(-1),
        }
    }

    fn next_bucket(&self, cluster: &Cluster, prev_bucket_id: i32) -> i32 {
        let old_bucket = self.current_bucket_id.load(Ordering::Relaxed);
        let mut new_bucket = old_bucket;
        if old_bucket < 0 || old_bucket == prev_bucket_id {
            let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
            if available_buckets.is_empty() {
                let mut rng = rand::rng();
                let mut random: i32 = rng.random();
                random &= i32::MAX;
                new_bucket = random % cluster.get_bucket_count(&self.table_path);
            } else if available_buckets.len() == 1 {
                new_bucket = available_buckets[0].table_bucket.bucket_id();
            } else {
                let mut rng = rand::rng();
                while new_bucket < 0 || new_bucket == old_bucket {
                    let mut random: i32 = rng.random();
                    random &= i32::MAX;
                    new_bucket = available_buckets
                        [(random % available_buckets.len() as i32) as usize]
                        .bucket_id();
                }
            }
        }

        if old_bucket < 0 {
            self.current_bucket_id.store(new_bucket, Ordering::Relaxed);
        } else {
            self.current_bucket_id
                .compare_exchange(
                    prev_bucket_id,
                    new_bucket,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
        self.current_bucket_id.load(Ordering::Relaxed)
    }
}

impl BucketAssigner for StickyBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        true
    }

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32) {
        self.next_bucket(cluster, prev_bucket_id);
    }

    fn assign_bucket(&self, _bucket_key: Option<&[u8]>, cluster: &Cluster) -> Result<i32> {
        let bucket_id = self.current_bucket_id.load(Ordering::Relaxed);
        if bucket_id < 0 {
            Ok(self.next_bucket(cluster, bucket_id))
        } else {
            Ok(bucket_id)
        }
    }
}

/// A [BucketAssigner] which assigns based on a modulo hashing function
pub struct HashBucketAssigner {
    num_buckets: i32,
    bucketing_function: Box<dyn BucketingFunction>,
}

#[allow(dead_code)]
impl HashBucketAssigner {
    /// Creates a new [HashBucketAssigner] based on the given [BucketingFunction].
    /// See [BucketingFunction.of(Option<&DataLakeFormat>)] for bucketing functions.
    ///
    ///
    /// # Arguments
    /// * `num_buckets` - The number of buckets
    /// * `bucketing_function` - The bucketing function
    ///
    /// # Returns
    /// * [HashBucketAssigner] - The hash bucket assigner
    pub fn new(num_buckets: i32, bucketing_function: Box<dyn BucketingFunction>) -> Self {
        HashBucketAssigner {
            num_buckets,
            bucketing_function,
        }
    }
}

impl BucketAssigner for HashBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }

    fn on_new_batch(&self, _: &Cluster, _: i32) {
        // do nothing
    }

    fn assign_bucket(&self, bucket_key: Option<&[u8]>, _: &Cluster) -> Result<i32> {
        let key = bucket_key.ok_or_else(|| IllegalArgument {
            message: "no bucket key provided".to_string(),
        })?;
        self.bucketing_function.bucketing(key, self.num_buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucketing::BucketingFunction;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::metadata::TableBucket;
    use crate::metadata::{DataField, DataTypes, Schema, TableDescriptor, TableInfo, TablePath};
    use std::collections::HashMap;

    fn build_table_info(table_path: TablePath, table_id: i64, buckets: i32) -> TableInfo {
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

    fn build_cluster(table_path: &TablePath, table_id: i64, buckets: i32) -> Cluster {
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

    #[test]
    fn sticky_bucket_assigner_picks_available_bucket() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster(&table_path, 1, 2);
        let assigner = StickyBucketAssigner::new(table_path);
        let bucket = assigner.assign_bucket(None, &cluster).expect("bucket");
        assert!(bucket >= 0 && bucket < 2);

        assigner.on_new_batch(&cluster, bucket);
        let next_bucket = assigner.assign_bucket(None, &cluster).expect("bucket");
        assert!(next_bucket >= 0 && next_bucket < 2);
    }

    #[test]
    fn hash_bucket_assigner_requires_key() {
        let assigner = HashBucketAssigner::new(3, <dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let err = assigner.assign_bucket(None, &cluster).unwrap_err();
        assert!(matches!(err, crate::error::Error::IllegalArgument { .. }));
    }

    #[test]
    fn hash_bucket_assigner_hashes_key() {
        let assigner = HashBucketAssigner::new(4, <dyn BucketingFunction>::of(None));
        let cluster = Cluster::default();
        let bucket = assigner
            .assign_bucket(Some(b"key"), &cluster)
            .expect("bucket");
        assert!(bucket >= 0 && bucket < 4);
    }
}
