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

//! Bounded log reader that polls until stopping offsets, then terminates.
//!
//! Unlike [`RecordBatchLogScanner`] which is unbounded (continuous streaming),
//! [`RecordBatchLogReader`] reads log data up to a finite set of stopping
//! offsets and then signals completion. This enables "snapshot-style" reads
//! from a streaming log: capture the latest offsets, then consume all data
//! up to those offsets.
//!
//! The reader also provides a synchronous [`arrow::record_batch::RecordBatchReader`]
//! adapter via [`RecordBatchLogReader::to_record_batch_reader`] for Arrow
//! ecosystem interop and FFI consumers (Python, C++).

use crate::client::admin::FlussAdmin;
use crate::client::table::RecordBatchLogScanner;
use crate::error::{Error, Result};
use crate::metadata::TableBucket;
use crate::record::ScanBatch;
use crate::rpc::message::OffsetSpec;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Bounded log reader that consumes log data up to specified stopping offsets.
///
/// This type wraps a [`RecordBatchLogScanner`] and adds stopping semantics:
/// it polls batches from the scanner, filters/slices them against per-bucket
/// stopping offsets, and signals completion when all buckets are caught up.
///
/// # Concurrent use (important)
///
/// [`RecordBatchLogScanner`] is cheaply clonable and all clones share the same
/// underlying scanner state (fetch buffer, subscription state, in-flight
/// fetches). A `RecordBatchLogReader` typically takes one clone while the
/// [`crate::client::FlussTable`] scan path may still hold another handle to the
/// same logical scanner.
///
/// **Do not** interleave [`RecordBatchLogScanner::poll`] (or Python
/// `poll_arrow` / `poll_record_batch`) with [`next_batch`](RecordBatchLogReader::next_batch)
/// on scanners that share this state. Use either the bounded reader **or** the
/// low-level poll loop for a given subscription session, not both at once.
/// Overlapping calls fail fast with [`crate::error::Error::UnsupportedOperation`]
/// (serialized in the client via `LogScannerInner::poll_session`).
///
/// # Construction
///
/// Use [`RecordBatchLogReader::new_until_latest`] for the common case of
/// reading all currently-available data, or [`RecordBatchLogReader::new_until_offsets`]
/// for custom stopping offsets.
///
/// # Async iteration
///
/// Call [`next_batch`](RecordBatchLogReader::next_batch) repeatedly to get
/// `RecordBatch`es lazily, one at a time. Returns `None` when all buckets
/// have reached their stopping offsets.
///
/// # Sync adapter
///
/// Call [`to_record_batch_reader`](RecordBatchLogReader::to_record_batch_reader)
/// to get a synchronous [`arrow::record_batch::RecordBatchReader`] suitable
/// for Arrow FFI consumers.
pub struct RecordBatchLogReader {
    scanner: RecordBatchLogScanner,
    stopping_offsets: HashMap<TableBucket, i64>,
    buffer: VecDeque<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchLogReader {
    /// Create a reader that reads until the latest offsets at the time of creation.
    ///
    /// Queries the server for the current latest offset of each subscribed
    /// bucket, then reads until those offsets are reached.
    pub async fn new_until_latest(
        scanner: RecordBatchLogScanner,
        admin: &FlussAdmin,
    ) -> Result<Self> {
        let subscribed = scanner.get_subscribed_buckets();
        if subscribed.is_empty() {
            return Err(Error::IllegalArgument {
                message: "No buckets subscribed. Call subscribe() before creating a reader."
                    .to_string(),
            });
        }

        let stopping_offsets = query_latest_offsets(admin, &scanner, &subscribed).await?;
        let schema = scanner.schema();

        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Create a reader with explicit stopping offsets per bucket.
    pub fn new_until_offsets(
        scanner: RecordBatchLogScanner,
        stopping_offsets: HashMap<TableBucket, i64>,
    ) -> Self {
        let schema = scanner.schema();
        Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        }
    }

    /// Returns the Arrow schema for batches produced by this reader.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Drain all remaining batches until stopping offsets are satisfied.
    ///
    /// This is a convenience for callers (e.g. bindings building a single Arrow
    /// table) that want to materialize the full result in Rust without per-batch
    /// Python iteration.
    pub async fn collect_all_batches(&mut self) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        while let Some(b) = self.next_batch().await? {
            out.push(b);
        }
        Ok(out)
    }

    /// Fetch the next `RecordBatch`, or `None` if all buckets are caught up.
    ///
    /// Each call may internally poll multiple batches from the scanner,
    /// buffer them, and return one at a time. Batches that cross a stopping
    /// offset boundary are sliced to exclude records at or beyond the stop point.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.buffer.pop_front() {
                return Ok(Some(batch));
            }

            if self.stopping_offsets.is_empty() {
                return Ok(None);
            }

            let scan_batches = self.scanner.poll(DEFAULT_POLL_TIMEOUT).await?;

            if scan_batches.is_empty() {
                continue;
            }

            filter_batches(scan_batches, &mut self.stopping_offsets, &mut self.buffer);
        }
    }

    /// Convert this async reader into a synchronous [`arrow::record_batch::RecordBatchReader`].
    ///
    /// The returned adapter calls [`tokio::runtime::Handle::block_on`] on each
    /// iterator step. **Do not** call this from inside a Tokio worker thread
    /// while the same runtime is driving async work (nested `block_on` can
    /// panic or deadlock). Prefer [`next_batch`](RecordBatchLogReader::next_batch)
    /// in async Rust code. This is intended for sync/FFI boundaries (C++, some
    /// Python call paths).
    pub fn to_record_batch_reader(
        self,
        handle: tokio::runtime::Handle,
    ) -> SyncRecordBatchLogReader {
        SyncRecordBatchLogReader {
            reader: self,
            handle,
        }
    }
}

/// Synchronous adapter that implements [`arrow::record_batch::RecordBatchReader`].
///
/// Created via [`RecordBatchLogReader::to_record_batch_reader`].
/// Blocks the current thread on each `next()` call using the provided
/// Tokio runtime handle.
pub struct SyncRecordBatchLogReader {
    reader: RecordBatchLogReader,
    handle: tokio::runtime::Handle,
}

impl Iterator for SyncRecordBatchLogReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handle.block_on(self.reader.next_batch()) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e)))),
        }
    }
}

impl arrow::record_batch::RecordBatchReader for SyncRecordBatchLogReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Query latest offsets for all subscribed buckets, handling both partitioned
/// and non-partitioned tables.
async fn query_latest_offsets(
    admin: &FlussAdmin,
    scanner: &RecordBatchLogScanner,
    subscribed: &[(TableBucket, i64)],
) -> Result<HashMap<TableBucket, i64>> {
    let table_path = scanner.table_path();

    if !scanner.is_partitioned() {
        let bucket_ids: Vec<i32> = subscribed.iter().map(|(tb, _)| tb.bucket_id()).collect();

        let offsets = admin
            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
            .await?;

        let table_id = scanner.table_id();
        Ok(offsets
            .into_iter()
            .filter(|(_, offset)| *offset > 0)
            .map(|(bucket_id, offset)| (TableBucket::new(table_id, bucket_id), offset))
            .collect())
    } else {
        query_partitioned_offsets(admin, scanner, subscribed).await
    }
}

/// Query offsets for partitioned table subscriptions.
async fn query_partitioned_offsets(
    admin: &FlussAdmin,
    scanner: &RecordBatchLogScanner,
    subscribed: &[(TableBucket, i64)],
) -> Result<HashMap<TableBucket, i64>> {
    let table_path = scanner.table_path();
    let table_id = scanner.table_id();

    let partition_infos = admin.list_partition_infos(table_path).await?;
    let partition_id_to_name: HashMap<i64, String> = partition_infos
        .into_iter()
        .map(|info| (info.get_partition_id(), info.get_partition_name()))
        .collect();

    let mut by_partition: HashMap<i64, Vec<i32>> = HashMap::new();
    for (tb, _) in subscribed {
        if let Some(partition_id) = tb.partition_id() {
            by_partition
                .entry(partition_id)
                .or_default()
                .push(tb.bucket_id());
        }
    }

    let mut result: HashMap<TableBucket, i64> = HashMap::new();

    for (partition_id, bucket_ids) in by_partition {
        let partition_name =
            partition_id_to_name
                .get(&partition_id)
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Unknown partition_id: {partition_id}"),
                    source: None,
                })?;

        let offsets = admin
            .list_partition_offsets(table_path, partition_name, &bucket_ids, OffsetSpec::Latest)
            .await?;

        for (bucket_id, offset) in offsets {
            if offset > 0 {
                let tb = TableBucket::new_with_partition(table_id, Some(partition_id), bucket_id);
                result.insert(tb, offset);
            }
        }
    }

    Ok(result)
}

/// Filter and slice scan batches against per-bucket stopping offsets.
///
/// For each batch:
/// - If the batch's bucket is not in `stopping_offsets`, skip it.
/// - If `base_offset >= stop_at`, the bucket is exhausted; remove from map.
/// - If `last_offset >= stop_at`, slice to keep only records before stop_at.
/// - Otherwise, keep the full batch.
///
/// Accepted batches are pushed to `buffer`. Exhausted buckets are removed
/// from `stopping_offsets`.
fn filter_batches(
    scan_batches: Vec<ScanBatch>,
    stopping_offsets: &mut HashMap<TableBucket, i64>,
    buffer: &mut VecDeque<RecordBatch>,
) {
    for scan_batch in scan_batches {
        let bucket = scan_batch.bucket().clone();
        let Some(&stop_at) = stopping_offsets.get(&bucket) else {
            continue;
        };

        let base_offset = scan_batch.base_offset();
        let last_offset = scan_batch.last_offset();

        if base_offset >= stop_at {
            stopping_offsets.remove(&bucket);
            continue;
        }

        let batch = if last_offset >= stop_at {
            let num_to_keep = (stop_at - base_offset) as usize;
            let b = scan_batch.into_batch();
            let limit = num_to_keep.min(b.num_rows());
            b.slice(0, limit)
        } else {
            scan_batch.into_batch()
        };

        buffer.push_back(batch);

        if last_offset >= stop_at - 1 {
            stopping_offsets.remove(&bucket);
        }
    }
}

// TODO: Add an end-to-end test with `FlussTestingCluster` (feature
// `integration_tests`) covering `new_until_latest`, partitioned tables, and
// `new_until_offsets` stopping semantics.
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn make_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn make_scan_batch(bucket: TableBucket, base_offset: i64, values: &[i32]) -> ScanBatch {
        ScanBatch::new(bucket, make_batch(values), base_offset)
    }

    fn bucket(id: i32) -> TableBucket {
        TableBucket::new(1, id)
    }

    #[test]
    fn filter_batch_entirely_before_stop() {
        let mut offsets = HashMap::from([(bucket(0), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].num_rows(), 3);
        assert!(offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_batch_crossing_stop_offset_is_sliced() {
        let mut offsets = HashMap::from([(bucket(0), 12)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, 5 rows -> offsets 10,11,12,13,14; stop_at=12 -> keep 2
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3, 4, 5])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].num_rows(), 2);
        assert!(!offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_batch_at_or_after_stop_offset_is_skipped() {
        let mut offsets = HashMap::from([(bucket(0), 10)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, stop_at=10 -> base >= stop, skip entirely
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(!offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_batch_ending_exactly_at_stop_minus_one() {
        let mut offsets = HashMap::from([(bucket(0), 13)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, 3 rows -> offsets 10,11,12; last_offset=12, stop_at=13
        // last_offset (12) >= stop_at - 1 (12) => bucket done
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].num_rows(), 3);
        assert!(!offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_unknown_bucket_is_ignored() {
        let mut offsets = HashMap::from([(bucket(0), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(99), 0, &[1, 2])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_multiple_buckets_independent_tracking() {
        let mut offsets = HashMap::from([(bucket(0), 12), (bucket(1), 5)]);
        let mut buffer = VecDeque::new();

        let batches = vec![
            make_scan_batch(bucket(0), 10, &[1, 2, 3]), // last=12, stop=12 -> keep 2, done
            make_scan_batch(bucket(1), 0, &[10, 20, 30]), // last=2, stop=5 -> keep all, not done
        ];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].num_rows(), 2); // bucket 0: sliced
        assert_eq!(buffer[1].num_rows(), 3); // bucket 1: full
        assert!(!offsets.contains_key(&bucket(0))); // bucket 0: done
        assert!(offsets.contains_key(&bucket(1))); // bucket 1: still tracking
    }

    #[test]
    fn filter_empty_batch_at_stop() {
        let mut offsets = HashMap::from([(bucket(0), 5)]);
        let mut buffer = VecDeque::new();

        // empty batch: base_offset=5, 0 rows -> last_offset = base-1 = 4
        // base_offset (5) >= stop_at (5) -> skip, remove
        let batches = vec![make_scan_batch(bucket(0), 5, &[])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(!offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_single_row_batch_before_stop() {
        let mut offsets = HashMap::from([(bucket(0), 10)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 5, &[42])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].num_rows(), 1);
        assert!(offsets.contains_key(&bucket(0)));
    }

    #[test]
    fn filter_single_row_batch_at_stop_boundary() {
        let mut offsets = HashMap::from([(bucket(0), 5)]);
        let mut buffer = VecDeque::new();

        // base_offset=4, 1 row -> last_offset=4, stop=5
        // last < stop -> keep all; last (4) >= stop-1 (4) -> done
        let batches = vec![make_scan_batch(bucket(0), 4, &[42])];
        filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].num_rows(), 1);
        assert!(!offsets.contains_key(&bucket(0)));
    }
}
