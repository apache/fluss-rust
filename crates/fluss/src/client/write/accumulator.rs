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

use crate::client::write::batch::WriteBatch::{ArrowLog, Kv};
use crate::client::write::batch::{ArrowLogWriteBatch, KvWriteBatch, WriteBatch};
use crate::client::{LogWriteRecord, Record, ResultHandle, WriteRecord};
use crate::cluster::{BucketLocation, Cluster, ServerNode};
use crate::config::Config;
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableBucket};
use crate::util::current_time_ms;
use crate::{BucketId, PartitionId, TableId};
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

// Type alias to simplify complex nested types
type BucketBatches = Vec<(BucketId, Arc<Mutex<VecDeque<WriteBatch>>>)>;

#[allow(dead_code)]
pub struct RecordAccumulator {
    config: Config,
    write_batches: DashMap<Arc<PhysicalTablePath>, BucketAndWriteBatches>,
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
        let batch_timeout_ms = config.writer_batch_timeout_ms;
        RecordAccumulator {
            config,
            write_batches: Default::default(),
            incomplete_batches: Default::default(),
            batch_timeout_ms,
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
        dq: &mut VecDeque<WriteBatch>,
    ) -> Result<RecordAppendResult> {
        if let Some(append_result) = self.try_append(record, dq)? {
            return Ok(append_result);
        }

        let physical_table_path = &record.physical_table_path;
        let table_path = physical_table_path.get_table_path();
        let table_info = cluster.get_table(table_path)?;
        let arrow_compression_info = table_info.get_table_config().get_arrow_compression_info()?;
        let row_type = &table_info.row_type;

        let schema_id = table_info.schema_id;

        let mut batch: WriteBatch = match record.record() {
            Record::Log(_) => ArrowLog(ArrowLogWriteBatch::new(
                self.batch_id.fetch_add(1, Ordering::Relaxed),
                Arc::clone(physical_table_path),
                schema_id,
                arrow_compression_info,
                row_type,
                current_time_ms(),
                matches!(&record.record, Record::Log(LogWriteRecord::RecordBatch(_))),
            )?),
            Record::Kv(kv_record) => Kv(KvWriteBatch::new(
                self.batch_id.fetch_add(1, Ordering::Relaxed),
                Arc::clone(physical_table_path),
                schema_id,
                // TODO: Decide how to derive write limit in the absence of java's equivalent of PreAllocatedPagedOutputView
                KvWriteBatch::DEFAULT_WRITE_LIMIT,
                record.write_format.to_kv_format()?,
                kv_record.target_columns.clone(),
                current_time_ms(),
            )),
        };

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

    pub fn append(
        &self,
        record: &WriteRecord<'_>,
        bucket_id: BucketId,
        cluster: &Cluster,
        abort_if_batch_full: bool,
    ) -> Result<RecordAppendResult> {
        let physical_table_path = &record.physical_table_path;
        let table_path = physical_table_path.get_table_path();
        let table_info = cluster.get_table(table_path)?;
        let is_partitioned_table = table_info.is_partitioned();

        let partition_id = if is_partitioned_table {
            cluster.get_partition_id(physical_table_path)
        } else {
            None
        };

        let dq = {
            let mut binding = self
                .write_batches
                .entry(Arc::clone(physical_table_path))
                .or_insert_with(|| BucketAndWriteBatches {
                    table_id: table_info.table_id,
                    is_partitioned_table,
                    partition_id,
                    batches: Default::default(),
                });
            let bucket_and_batches = binding.value_mut();
            bucket_and_batches
                .batches
                .entry(bucket_id)
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone()
        };

        let mut dq_guard = dq.lock();
        if let Some(append_result) = self.try_append(record, &mut dq_guard)? {
            return Ok(append_result);
        }

        if abort_if_batch_full {
            return Ok(RecordAppendResult::new_without_result_handle(
                true, false, true,
            ));
        }
        self.append_new_batch(cluster, record, &mut dq_guard)
    }

    pub fn ready(&self, cluster: &Arc<Cluster>) -> Result<ReadyCheckResult> {
        // Snapshot just the Arcs we need, avoiding cloning the entire BucketAndWriteBatches struct
        let entries: Vec<(Arc<PhysicalTablePath>, Option<PartitionId>, BucketBatches)> = self
            .write_batches
            .iter()
            .map(|entry| {
                let physical_table_path = Arc::clone(entry.key());
                let partition_id = entry.value().partition_id;
                let bucket_batches: Vec<_> = entry
                    .value()
                    .batches
                    .iter()
                    .map(|(bucket_id, batch_arc)| (*bucket_id, batch_arc.clone()))
                    .collect();
                (physical_table_path, partition_id, bucket_batches)
            })
            .collect();

        let mut ready_nodes = HashSet::new();
        let mut next_ready_check_delay_ms = self.batch_timeout_ms;
        let mut unknown_leader_tables = HashSet::new();

        for (physical_table_path, mut partition_id, bucket_batches) in entries {
            next_ready_check_delay_ms = self.bucket_ready(
                &physical_table_path,
                physical_table_path.get_partition_name().is_some(),
                &mut partition_id,
                bucket_batches,
                &mut ready_nodes,
                &mut unknown_leader_tables,
                cluster,
                next_ready_check_delay_ms,
            )?
        }

        Ok(ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn bucket_ready(
        &self,
        physical_table_path: &Arc<PhysicalTablePath>,
        is_partitioned_table: bool,
        partition_id: &mut Option<PartitionId>,
        bucket_batches: BucketBatches,
        ready_nodes: &mut HashSet<ServerNode>,
        unknown_leader_tables: &mut HashSet<Arc<PhysicalTablePath>>,
        cluster: &Cluster,
        next_ready_check_delay_ms: i64,
    ) -> Result<i64> {
        let mut next_delay = next_ready_check_delay_ms;

        // First check this table has partitionId.
        if is_partitioned_table && partition_id.is_none() {
            let partition_id = cluster.get_partition_id(physical_table_path);

            if partition_id.is_some() {
                // Update the cached partition_id
                if let Some(mut entry) = self.write_batches.get_mut(physical_table_path) {
                    entry.partition_id = partition_id;
                }
            } else {
                log::debug!(
                    "Partition does not exist for {}, bucket will not be set to ready",
                    physical_table_path.as_ref()
                );

                // TODO: we shouldn't add unready partitions to unknownLeaderTables,
                // because it cases PartitionNotExistException later
                unknown_leader_tables.insert(Arc::clone(physical_table_path));
                return Ok(next_delay);
            }
        }

        for (bucket_id, batch) in bucket_batches {
            let batch_guard = batch.lock();
            if batch_guard.is_empty() {
                continue;
            }

            let batch = batch_guard.front().unwrap();
            let waited_time_ms = batch.waited_time_ms(current_time_ms());
            let deque_size = batch_guard.len();
            let full = deque_size > 1 || batch.is_closed();
            let table_bucket = cluster.get_table_bucket(physical_table_path, bucket_id)?;
            if let Some(leader) = cluster.leader_for(&table_bucket) {
                next_delay =
                    self.batch_ready(leader, waited_time_ms, full, ready_nodes, next_delay);
            } else {
                unknown_leader_tables.insert(Arc::clone(physical_table_path));
            }
        }
        Ok(next_delay)
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

    pub fn drain(
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
            let ready = self.drain_batches_for_one_node(&cluster, node, max_size)?;
            if !ready.is_empty() {
                batches.insert(node.id(), ready);
            }
        }

        Ok(batches)
    }

    fn drain_batches_for_one_node(
        &self,
        cluster: &Cluster,
        node: &ServerNode,
        max_size: i32,
    ) -> Result<Vec<ReadyWriteBatch>> {
        let mut size: usize = 0;
        let buckets = self.get_all_buckets_in_current_node(node, cluster);
        let mut ready = Vec::new();

        if buckets.is_empty() {
            return Ok(ready);
        }

        let start = {
            let mut nodes_drain_index_guard = self.nodes_drain_index.lock();
            let drain_index = nodes_drain_index_guard.entry(node.id()).or_insert(0);
            *drain_index % buckets.len()
        };

        let mut current_index = start;
        let mut last_processed_index;

        loop {
            let bucket = &buckets[current_index];
            let table_path = bucket.physical_table_path();
            let table_bucket = bucket.table_bucket.clone();
            last_processed_index = current_index;
            current_index = (current_index + 1) % buckets.len();

            let deque = self
                .write_batches
                .get(table_path)
                .and_then(|bucket_and_write_batches| {
                    bucket_and_write_batches
                        .batches
                        .get(&table_bucket.bucket_id())
                        .cloned()
                });

            if let Some(deque) = deque {
                let mut maybe_batch = None;
                {
                    let mut batch_lock = deque.lock();
                    if !batch_lock.is_empty() {
                        let first_batch = batch_lock.front().unwrap();

                        if size + first_batch.estimated_size_in_bytes() > max_size as usize
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
            if current_index == start {
                break;
            }
        }

        // Store the last processed index to maintain round-robin fairness
        {
            let mut nodes_drain_index_guard = self.nodes_drain_index.lock();
            nodes_drain_index_guard.insert(node.id(), last_processed_index);
        }

        Ok(ready)
    }

    pub fn remove_incomplete_batches(&self, batch_id: i64) {
        self.incomplete_batches.write().remove(&batch_id);
    }

    pub fn re_enqueue(&self, ready_write_batch: ReadyWriteBatch) {
        ready_write_batch.write_batch.re_enqueued();
        let physical_table_path = ready_write_batch.write_batch.physical_table_path();
        let bucket_id = ready_write_batch.table_bucket.bucket_id();
        let table_id = ready_write_batch.table_bucket.table_id();
        let partition_id = ready_write_batch.table_bucket.partition_id();
        let is_partitioned_table = partition_id.is_some();

        let dq = {
            let mut binding = self
                .write_batches
                .entry(Arc::clone(physical_table_path))
                .or_insert_with(|| BucketAndWriteBatches {
                    table_id,
                    is_partitioned_table,
                    partition_id,
                    batches: Default::default(),
                });
            let bucket_and_batches = binding.value_mut();
            bucket_and_batches
                .batches
                .entry(bucket_id)
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone()
        };

        let mut dq_guard = dq.lock();
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
    pub async fn await_flush_completion(&self) -> Result<()> {
        // Clone handles before awaiting to avoid holding RwLock read guard across await points
        let handles: Vec<_> = self.incomplete_batches.read().values().cloned().collect();

        // Await on all handles
        let result = async {
            for result_handle in handles {
                result_handle.wait().await?;
            }
            Ok(())
        }
        .await;

        // Always decrement flushes_in_progress, even if an error occurred
        // This mimics the Java finally block behavior
        self.flushes_in_progress.fetch_sub(1, Ordering::SeqCst);

        result
    }
}

pub struct ReadyWriteBatch {
    pub table_bucket: TableBucket,
    pub write_batch: WriteBatch,
}

impl ReadyWriteBatch {
    pub fn write_batch(&self) -> &WriteBatch {
        &self.write_batch
    }
}

#[allow(dead_code)]
struct BucketAndWriteBatches {
    table_id: TableId,
    is_partitioned_table: bool,
    partition_id: Option<PartitionId>,
    batches: HashMap<BucketId, Arc<Mutex<VecDeque<WriteBatch>>>>,
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
    pub unknown_leader_tables: HashSet<Arc<PhysicalTablePath>>,
}

impl ReadyCheckResult {
    pub fn new(
        ready_nodes: HashSet<ServerNode>,
        next_ready_check_delay_ms: i64,
        unknown_leader_tables: HashSet<Arc<PhysicalTablePath>>,
    ) -> Self {
        ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TablePath;
    use crate::row::{Datum, GenericRow};
    use crate::test_utils::{build_cluster, build_table_info};
    use std::sync::Arc;

    #[tokio::test]
    async fn re_enqueue_increments_attempts() -> Result<()> {
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);

        accumulator.append(&record, 0, &cluster, false)?;

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 0);

        accumulator.re_enqueue(batch);

        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn flush_counter_decremented_on_error() -> Result<()> {
        use crate::client::write::broadcast::BroadcastOnce;
        use std::sync::atomic::Ordering;

        let config = Config::default();
        let accumulator = RecordAccumulator::new(config);

        accumulator.begin_flush();
        assert_eq!(accumulator.flushes_in_progress.load(Ordering::SeqCst), 1);

        // Create a failing batch by dropping the BroadcastOnce without broadcasting
        {
            let broadcast = BroadcastOnce::default();
            let receiver = broadcast.receiver();
            let handle = ResultHandle::new(receiver);
            accumulator.incomplete_batches.write().insert(1, handle);
            // broadcast is dropped here, causing an error
        }

        // Await flush completion should fail but still decrement counter
        let result = accumulator.await_flush_completion().await;
        assert!(result.is_err());

        // Counter should still be decremented (this is the critical fix!)
        assert_eq!(accumulator.flushes_in_progress.load(Ordering::SeqCst), 0);
        assert!(!accumulator.flush_in_progress());

        Ok(())
    }
}
