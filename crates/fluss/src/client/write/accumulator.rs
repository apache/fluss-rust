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

use crate::client::write::batch::WriteBatch::ArrowLog;
use crate::client::write::batch::{ArrowLogWriteBatch, WriteBatch};
use crate::client::{Record, ResultHandle, WriteRecord};
use crate::cluster::{BucketLocation, Cluster, ServerNode};
use crate::config::Config;
use crate::error::Result;
use crate::metadata::{TableBucket, TablePath};
use crate::util::current_time_ms;
use crate::{BucketId, PartitionId, TableId};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use tokio::sync::Mutex;

#[allow(dead_code)]
pub struct RecordAccumulator {
    config: Config,
    write_batches: DashMap<TablePath, BucketAndWriteBatches>,
    // batch_id -> complete callback
    incomplete_batches: RwLock<HashMap<i64, ResultHandle>>,
    batch_timeout_ms: i64,
    closed: bool,
    flushes_in_progress: AtomicI32,
    appends_in_progress: i32,
    nodes_drain_index: Mutex<HashMap<i32, usize>>,
    batch_id: AtomicI64,
}

impl RecordAccumulator {
    pub fn new(config: Config) -> Self {
        RecordAccumulator {
            config,
            write_batches: Default::default(),
            incomplete_batches: Default::default(),
            batch_timeout_ms: 500,
            closed: Default::default(),
            flushes_in_progress: Default::default(),
            appends_in_progress: Default::default(),
            nodes_drain_index: Default::default(),
            batch_id: Default::default(),
        }
    }

    fn try_append(
        &self,
        record: &WriteRecord,
        dq: &mut VecDeque<WriteBatch>,
    ) -> Result<Option<RecordAppendResult>> {
        let dq_size = dq.len();
        if let Some(last_batch) = dq.back_mut() {
            return if let Some(result_handle) = last_batch.try_append(record)? {
                Ok(Some(RecordAppendResult::new(
                    result_handle,
                    dq_size > 1 || last_batch.is_closed(),
                    false,
                    false,
                )))
            } else {
                Ok(None)
            };
        }
        Ok(None)
    }

    fn append_new_batch(
        &self,
        cluster: &Cluster,
        record: &WriteRecord,
        bucket_id: BucketId,
        dq: &mut VecDeque<WriteBatch>,
    ) -> Result<RecordAppendResult> {
        if let Some(append_result) = self.try_append(record, dq)? {
            return Ok(append_result);
        }

        let table_path = &record.table_path;
        let table_info = cluster.get_table(table_path);
        let arrow_compression_info = table_info.get_table_config().get_arrow_compression_info()?;
        let row_type = &cluster.get_table(table_path).row_type;

        let schema_id = table_info.schema_id;

        let mut batch = ArrowLog(ArrowLogWriteBatch::new(
            self.batch_id.fetch_add(1, Ordering::Relaxed),
            table_path.as_ref().clone(),
            schema_id,
            arrow_compression_info,
            row_type,
            bucket_id,
            current_time_ms(),
            matches!(record.row, Record::RecordBatch(_)),
        ));

        let batch_id = batch.batch_id();

        let result_handle = batch
            .try_append(record)?
            .expect("must append to a new batch");

        let batch_is_closed = batch.is_closed();
        dq.push_back(batch);

        self.incomplete_batches
            .write()
            .insert(batch_id, result_handle.clone());
        Ok(RecordAppendResult::new(
            result_handle,
            dq.len() > 1 || batch_is_closed,
            true,
            false,
        ))
    }

    pub async fn append(
        &self,
        record: &WriteRecord<'_>,
        bucket_id: BucketId,
        cluster: &Cluster,
        abort_if_batch_full: bool,
    ) -> Result<RecordAppendResult> {
        let table_path = &record.table_path;
        let mut binding = self
            .write_batches
            .entry(table_path.as_ref().clone())
            .or_insert_with(|| BucketAndWriteBatches {
                table_id: 0,
                is_partitioned_table: false,
                partition_id: None,
                batches: Default::default(),
            });
        let bucket_and_batches = binding.value_mut();
        let dq = bucket_and_batches
            .batches
            .entry(bucket_id)
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut dq_guard = dq.lock().await;
        if let Some(append_result) = self.try_append(record, &mut dq_guard)? {
            return Ok(append_result);
        }

        if abort_if_batch_full {
            return Ok(RecordAppendResult::new_without_result_handle(
                true, false, true,
            ));
        }
        self.append_new_batch(cluster, record, bucket_id, &mut dq_guard)
    }

    pub async fn ready(&self, cluster: &Arc<Cluster>) -> ReadyCheckResult {
        let mut ready_nodes = HashSet::new();
        let mut next_ready_check_delay_ms = self.batch_timeout_ms;
        let mut unknown_leader_tables = HashSet::new();
        for entry in self.write_batches.iter() {
            let table_path = entry.key();
            let batches = entry.value();
            next_ready_check_delay_ms = self
                .bucket_ready(
                    table_path,
                    batches,
                    &mut ready_nodes,
                    &mut unknown_leader_tables,
                    cluster,
                    next_ready_check_delay_ms,
                )
                .await
        }

        ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        }
    }

    async fn bucket_ready(
        &self,
        table_path: &TablePath,
        batches: &BucketAndWriteBatches,
        ready_nodes: &mut HashSet<ServerNode>,
        unknown_leader_tables: &mut HashSet<TablePath>,
        cluster: &Cluster,
        next_ready_check_delay_ms: i64,
    ) -> i64 {
        let mut next_delay = next_ready_check_delay_ms;

        for (bucket_id, batch) in batches.batches.iter() {
            let batch_guard = batch.lock().await;
            if batch_guard.is_empty() {
                continue;
            }

            let batch = batch_guard.front().unwrap();
            let waited_time_ms = batch.waited_time_ms(current_time_ms());
            let deque_size = batch_guard.len();
            let full = deque_size > 1 || batch.is_closed();
            let table_bucket = cluster.get_table_bucket(table_path, *bucket_id);
            if let Some(leader) = cluster.leader_for(&table_bucket) {
                next_delay =
                    self.batch_ready(leader, waited_time_ms, full, ready_nodes, next_delay);
            } else {
                unknown_leader_tables.insert(table_path.clone());
            }
        }
        next_delay
    }

    fn batch_ready(
        &self,
        leader: &ServerNode,
        waited_time_ms: i64,
        full: bool,
        ready_nodes: &mut HashSet<ServerNode>,
        next_ready_check_delay_ms: i64,
    ) -> i64 {
        if !ready_nodes.contains(leader) {
            let expired = waited_time_ms >= self.batch_timeout_ms;
            let sendable = full || expired || self.closed || self.flush_in_progress();

            if sendable {
                ready_nodes.insert(leader.clone());
            } else {
                let time_left_ms = self.batch_timeout_ms.saturating_sub(waited_time_ms);
                return next_ready_check_delay_ms.min(time_left_ms);
            }
        }
        next_ready_check_delay_ms
    }

    pub async fn drain(
        &self,
        cluster: Arc<Cluster>,
        nodes: &HashSet<ServerNode>,
        max_size: i32,
    ) -> Result<HashMap<i32, Vec<ReadyWriteBatch>>> {
        if nodes.is_empty() {
            return Ok(HashMap::new());
        }
        let mut batches = HashMap::new();
        for node in nodes {
            let ready = self
                .drain_batches_for_one_node(&cluster, node, max_size)
                .await?;
            if !ready.is_empty() {
                batches.insert(node.id(), ready);
            }
        }

        Ok(batches)
    }

    async fn drain_batches_for_one_node(
        &self,
        cluster: &Cluster,
        node: &ServerNode,
        max_size: i32,
    ) -> Result<Vec<ReadyWriteBatch>> {
        let mut size = 0;
        let buckets = self.get_all_buckets_in_current_node(node, cluster);
        let mut ready = Vec::new();

        if buckets.is_empty() {
            return Ok(ready);
        }

        let mut nodes_drain_index_guard = self.nodes_drain_index.lock().await;
        let drain_index = nodes_drain_index_guard.entry(node.id()).or_insert(0);
        let start = *drain_index % buckets.len();
        let mut current_index = start;

        loop {
            let bucket = &buckets[current_index];
            let table_path = bucket.table_path.clone();
            let table_bucket = bucket.table_bucket.clone();
            nodes_drain_index_guard.insert(node.id(), current_index);
            current_index = (current_index + 1) % buckets.len();

            let bucket_and_write_batches = self.write_batches.get(&table_path);
            if let Some(bucket_and_write_batches) = bucket_and_write_batches {
                if let Some(deque) = bucket_and_write_batches
                    .batches
                    .get(&table_bucket.bucket_id())
                {
                    let mut maybe_batch = None;
                    {
                        let mut batch_lock = deque.lock().await;
                        if !batch_lock.is_empty() {
                            let first_batch = batch_lock.front().unwrap();

                            if size + first_batch.estimated_size_in_bytes() > max_size as i64
                                && !ready.is_empty()
                            {
                                // there is a rare case that a single batch size is larger than the request size
                                // due to compression; in this case we will still eventually send this batch in
                                // a single request.
                                break;
                            }

                            maybe_batch = Some(batch_lock.pop_front().unwrap());
                        }
                    }

                    if let Some(mut batch) = maybe_batch {
                        let current_batch_size = batch.estimated_size_in_bytes();
                        size += current_batch_size;

                        // mark the batch as drained.
                        batch.drained(current_time_ms());
                        ready.push(ReadyWriteBatch {
                            table_bucket,
                            write_batch: batch,
                        });
                    }
                }
            }
            if current_index == start {
                break;
            }
        }
        Ok(ready)
    }

    pub fn remove_incomplete_batches(&self, batch_id: i64) {
        self.incomplete_batches.write().remove(&batch_id);
    }

    pub async fn re_enqueue(&self, ready_write_batch: ReadyWriteBatch) {
        ready_write_batch.write_batch.re_enqueued();
        let table_path = ready_write_batch.write_batch.table_path().clone();
        let bucket_id = ready_write_batch.table_bucket.bucket_id();
        let table_id = u64::try_from(ready_write_batch.table_bucket.table_id()).unwrap_or(0);
        let mut binding =
            self.write_batches
                .entry(table_path)
                .or_insert_with(|| BucketAndWriteBatches {
                    table_id,
                    is_partitioned_table: false,
                    partition_id: None,
                    batches: Default::default(),
                });
        let bucket_and_batches = binding.value_mut();
        let dq = bucket_and_batches
            .batches
            .entry(bucket_id)
            .or_insert_with(|| Mutex::new(VecDeque::new()));
        let mut dq_guard = dq.lock().await;
        dq_guard.push_front(ready_write_batch.write_batch);
    }

    fn get_all_buckets_in_current_node(
        &self,
        current: &ServerNode,
        cluster: &Cluster,
    ) -> Vec<BucketLocation> {
        let mut buckets = vec![];
        for bucket_locations in cluster.get_bucket_locations_by_path().values() {
            for bucket_location in bucket_locations {
                if let Some(leader) = bucket_location.leader() {
                    if current.id() == leader.id() {
                        buckets.push(bucket_location.clone());
                    }
                }
            }
        }
        buckets
    }

    fn flush_in_progress(&self) -> bool {
        self.flushes_in_progress.load(Ordering::SeqCst) > 0
    }

    pub fn begin_flush(&self) {
        self.flushes_in_progress.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(unused_must_use)]
    #[allow(clippy::await_holding_lock)]
    pub async fn await_flush_completion(&self) -> Result<()> {
        for result_handle in self.incomplete_batches.read().values() {
            result_handle.wait().await?;
        }
        Ok(())
    }
}

pub struct ReadyWriteBatch {
    pub table_bucket: TableBucket,
    pub write_batch: WriteBatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::metadata::{DataField, DataTypes, Schema, TableDescriptor, TableInfo, TablePath};
    use crate::row::{Datum, GenericRow};
    use std::sync::Arc;

    fn build_table_info(table_path: TablePath, table_id: i64) -> TableInfo {
        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let mut schema_builder = Schema::builder().with_row_type(&row_type);
        let schema = schema_builder.build().expect("schema build");
        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor build");
        TableInfo::of(table_path, table_id, 1, table_descriptor, 0, 0)
    }

    fn build_cluster(table_path: &TablePath, table_id: i64) -> Cluster {
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let table_bucket = TableBucket::new(table_id, 0);
        let bucket_location = BucketLocation::new(
            table_bucket.clone(),
            Some(server.clone()),
            table_path.clone(),
        );

        let mut servers = HashMap::new();
        servers.insert(server.id(), server);

        let mut locations_by_path = HashMap::new();
        locations_by_path.insert(table_path.clone(), vec![bucket_location.clone()]);

        let mut locations_by_bucket = HashMap::new();
        locations_by_bucket.insert(table_bucket, bucket_location);

        let mut table_id_by_path = HashMap::new();
        table_id_by_path.insert(table_path.clone(), table_id);

        let mut table_info_by_path = HashMap::new();
        table_info_by_path.insert(
            table_path.clone(),
            build_table_info(table_path.clone(), table_id),
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

    #[tokio::test]
    async fn re_enqueue_increments_attempts() -> Result<()> {
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config);
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = Arc::new(build_cluster(table_path.as_ref(), 1));
        let record = WriteRecord::new(
            table_path.clone(),
            GenericRow {
                values: vec![Datum::Int32(1)],
            },
        );

        accumulator.append(&record, 0, &cluster, false).await?;

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator
            .drain(cluster.clone(), &nodes, 1024 * 1024)
            .await?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 0);

        accumulator.re_enqueue(batch).await;

        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024).await?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 1);
        Ok(())
    }
}

#[allow(dead_code)]
struct BucketAndWriteBatches {
    table_id: TableId,
    is_partitioned_table: bool,
    partition_id: Option<PartitionId>,
    batches: HashMap<BucketId, Mutex<VecDeque<WriteBatch>>>,
}

pub struct RecordAppendResult {
    pub batch_is_full: bool,
    pub new_batch_created: bool,
    pub abort_record_for_new_batch: bool,
    pub result_handle: Option<ResultHandle>,
}

impl RecordAppendResult {
    fn new(
        result_handle: ResultHandle,
        batch_is_full: bool,
        new_batch_created: bool,
        abort_record_for_new_batch: bool,
    ) -> Self {
        Self {
            batch_is_full,
            new_batch_created,
            abort_record_for_new_batch,
            result_handle: Some(result_handle),
        }
    }

    fn new_without_result_handle(
        batch_is_full: bool,
        new_batch_created: bool,
        abort_record_for_new_batch: bool,
    ) -> Self {
        Self {
            batch_is_full,
            new_batch_created,
            abort_record_for_new_batch,
            result_handle: None,
        }
    }
}

pub struct ReadyCheckResult {
    pub ready_nodes: HashSet<ServerNode>,
    pub next_ready_check_delay_ms: i64,
    pub unknown_leader_tables: HashSet<TablePath>,
}

impl ReadyCheckResult {
    pub fn new(
        ready_nodes: HashSet<ServerNode>,
        next_ready_check_delay_ms: i64,
        unknown_leader_tables: HashSet<TablePath>,
    ) -> Self {
        ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        }
    }
}
