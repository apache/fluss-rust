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

use crate::error::{ApiError, Error, Result};
use arrow::array::RecordBatch;
use parking_lot::Mutex;

use crate::metadata::TableBucket;
use crate::record::{
    LogRecordBatch, LogRecordIterator, LogRecordsBatches, ReadContext, ScanRecord,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

/// Represents a completed fetch that can be consumed
pub trait CompletedFetch: Send + Sync {
    fn table_bucket(&self) -> &TableBucket;
    fn api_error(&self) -> Option<&ApiError>;
    fn take_error(&mut self) -> Option<Error>;
    fn fetch_records(&mut self, max_records: usize) -> Result<Vec<ScanRecord>>;
    fn fetch_batches(&mut self, max_batches: usize) -> Result<Vec<RecordBatch>>;
    fn is_consumed(&self) -> bool;
    fn records_read(&self) -> usize;
    fn drain(&mut self);
    fn size_in_bytes(&self) -> usize;
    fn high_watermark(&self) -> i64;
    fn is_initialized(&self) -> bool;
    fn set_initialized(&mut self);
    fn next_fetch_offset(&self) -> i64;
}

/// Represents a pending fetch that is waiting to be completed
pub trait PendingFetch: Send + Sync {
    fn table_bucket(&self) -> &TableBucket;
    fn is_completed(&self) -> bool;
    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>>;
}

/// Thread-safe buffer for completed fetches
pub struct LogFetchBuffer {
    completed_fetches: Mutex<VecDeque<Box<dyn CompletedFetch>>>,
    pending_fetches: Mutex<HashMap<TableBucket, VecDeque<Box<dyn PendingFetch>>>>,
    next_in_line_fetch: Mutex<Option<Box<dyn CompletedFetch>>>,
    not_empty_notify: Notify,
    woken_up: Arc<AtomicBool>,
}

impl LogFetchBuffer {
    pub fn new() -> Self {
        Self {
            completed_fetches: Mutex::new(VecDeque::new()),
            pending_fetches: Mutex::new(HashMap::new()),
            next_in_line_fetch: Mutex::new(None),
            not_empty_notify: Notify::new(),
            woken_up: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.completed_fetches.lock().is_empty()
    }

    /// Wait for the buffer to become non-empty, with timeout.
    /// Returns true if data became available, false if timeout.
    pub async fn await_not_empty(&self, timeout: Duration) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;

        loop {
            // Check if buffer is not empty
            if !self.is_empty() {
                return Ok(true);
            }

            // Check if woken up
            if self.woken_up.swap(false, Ordering::Acquire) {
                return Err(Error::WakeupError {
                    message: "The await is wakeup.".to_string(),
                });
            }

            // Check if timeout
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }

            // Wait for notification with remaining time
            let remaining = deadline - now;
            let notified = self.not_empty_notify.notified();
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {
                    return Ok(false); // Timeout
                }
                _ = notified => {
                    // Got notification, check again
                    continue;
                }
            }
        }
    }

    #[allow(dead_code)]
    /// Wake up any waiting threads
    pub fn wakeup(&self) {
        self.woken_up.store(true, Ordering::Release);
        self.not_empty_notify.notify_waiters();
    }

    pub(crate) fn set_error(&self, table_bucket: TableBucket, error: Error, fetch_offset: i64) {
        let error_fetch = ErrorCompletedFetch::from_error(table_bucket, error, fetch_offset);
        self.completed_fetches
            .lock()
            .push_back(Box::new(error_fetch));
        self.not_empty_notify.notify_waiters();
    }

    pub(crate) fn add_api_error(
        &self,
        table_bucket: TableBucket,
        api_error: ApiError,
        fetch_offset: i64,
    ) {
        let error_fetch =
            ErrorCompletedFetch::from_api_error(table_bucket, api_error, fetch_offset);
        self.completed_fetches
            .lock()
            .push_back(Box::new(error_fetch));
        self.not_empty_notify.notify_waiters();
    }

    /// Add a pending fetch to the buffer
    pub fn pend(&self, pending_fetch: Box<dyn PendingFetch>) {
        let table_bucket = pending_fetch.table_bucket().clone();
        self.pending_fetches
            .lock()
            .entry(table_bucket)
            .or_default()
            .push_back(pending_fetch);
    }

    /// Try to complete pending fetches in order, converting them to completed fetches
    pub fn try_complete(&self, table_bucket: &TableBucket) {
        // Collect completed fetches while holding the pending_fetches lock,
        // then push them to completed_fetches after releasing it to avoid
        // holding both locks simultaneously.
        let mut completed_to_push: Vec<Box<dyn CompletedFetch>> = Vec::new();
        let mut has_completed = false;
        let mut pending_error: Option<Error> = None;
        {
            let mut pending_map = self.pending_fetches.lock();
            if let Some(pendings) = pending_map.get_mut(table_bucket) {
                while let Some(front) = pendings.front() {
                    if front.is_completed() {
                        let pending = pendings.pop_front().unwrap();
                        match pending.to_completed_fetch() {
                            Ok(completed) => {
                                completed_to_push.push(completed);
                                has_completed = true;
                            }
                            Err(e) => {
                                pending_error = Some(e);
                                has_completed = true;
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                if has_completed && pendings.is_empty() {
                    pending_map.remove(table_bucket);
                }
            }
        }

        if let Some(error) = pending_error {
            let error_fetch = ErrorCompletedFetch::from_error(table_bucket.clone(), error, -1);
            completed_to_push.push(Box::new(error_fetch));
        }

        if !completed_to_push.is_empty() {
            let mut completed_queue = self.completed_fetches.lock();
            for completed in completed_to_push {
                completed_queue.push_back(completed);
            }
            has_completed = true;
        }

        if has_completed {
            // Signal that buffer is not empty
            self.not_empty_notify.notify_waiters();
        }
    }

    /// Add a completed fetch to the buffer
    pub fn add(&self, completed_fetch: Box<dyn CompletedFetch>) {
        let table_bucket = completed_fetch.table_bucket();
        let mut pending_map = self.pending_fetches.lock();

        if let Some(pendings) = pending_map.get_mut(table_bucket)
            && !pendings.is_empty()
        {
            pendings.push_back(Box::new(CompletedPendingFetch::new(completed_fetch)));
            return;
        }
        // If there's no pending fetch for this table_bucket,
        // directly add to completed_fetches
        self.completed_fetches.lock().push_back(completed_fetch);
        self.not_empty_notify.notify_waiters();
    }

    /// Poll the next completed fetch
    pub fn poll(&self) -> Option<Box<dyn CompletedFetch>> {
        self.completed_fetches.lock().pop_front()
    }

    /// Get the next in line fetch
    pub fn next_in_line_fetch(&self) -> Option<Box<dyn CompletedFetch>> {
        self.next_in_line_fetch.lock().take()
    }

    /// Set the next in line fetch
    pub fn set_next_in_line_fetch(&self, fetch: Option<Box<dyn CompletedFetch>>) {
        *self.next_in_line_fetch.lock() = fetch;
    }

    /// Get the set of buckets that have buffered data
    pub fn buffered_buckets(&self) -> Vec<TableBucket> {
        let mut buckets = Vec::new();

        let next_in_line_fetch = self.next_in_line_fetch.lock();
        if let Some(complete_fetch) = next_in_line_fetch.as_ref() {
            if !complete_fetch.is_consumed() {
                buckets.push(complete_fetch.table_bucket().clone());
            }
        }

        let completed = self.completed_fetches.lock();
        for fetch in completed.iter() {
            buckets.push(fetch.table_bucket().clone());
        }
        let pending = self.pending_fetches.lock();
        buckets.extend(pending.keys().cloned());
        buckets
    }
}

impl Default for LogFetchBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// A wrapper that makes a completed fetch look like a pending fetch
struct CompletedPendingFetch {
    completed_fetch: Box<dyn CompletedFetch>,
}

impl CompletedPendingFetch {
    fn new(completed_fetch: Box<dyn CompletedFetch>) -> Self {
        Self { completed_fetch }
    }
}

impl PendingFetch for CompletedPendingFetch {
    fn table_bucket(&self) -> &TableBucket {
        self.completed_fetch.table_bucket()
    }

    fn is_completed(&self) -> bool {
        true
    }

    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
        Ok(self.completed_fetch)
    }
}

struct ErrorCompletedFetch {
    table_bucket: TableBucket,
    api_error: Option<ApiError>,
    error: Option<Error>,
    next_fetch_offset: i64,
    consumed: bool,
    initialized: bool,
}

impl ErrorCompletedFetch {
    fn from_error(table_bucket: TableBucket, error: Error, fetch_offset: i64) -> Self {
        Self {
            table_bucket,
            api_error: None,
            error: Some(error),
            next_fetch_offset: fetch_offset,
            consumed: false,
            initialized: false,
        }
    }

    fn from_api_error(table_bucket: TableBucket, api_error: ApiError, fetch_offset: i64) -> Self {
        Self {
            table_bucket,
            api_error: Some(api_error),
            error: None,
            next_fetch_offset: fetch_offset,
            consumed: false,
            initialized: false,
        }
    }
}

impl CompletedFetch for ErrorCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn api_error(&self) -> Option<&ApiError> {
        self.api_error.as_ref()
    }

    fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    fn fetch_records(&mut self, _max_records: usize) -> Result<Vec<ScanRecord>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        if let Some(api_error) = self.api_error.as_ref() {
            return Err(Error::FlussAPIError {
                api_error: ApiError {
                    code: api_error.code,
                    message: api_error.message.clone(),
                },
            });
        }

        Ok(Vec::new())
    }

    fn fetch_batches(&mut self, _max_batches: usize) -> Result<Vec<RecordBatch>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        if let Some(api_error) = self.api_error.as_ref() {
            return Err(Error::FlussAPIError {
                api_error: ApiError {
                    code: api_error.code,
                    message: api_error.message.clone(),
                },
            });
        }

        Ok(Vec::new())
    }

    fn is_consumed(&self) -> bool {
        self.consumed
    }

    fn records_read(&self) -> usize {
        0
    }

    fn drain(&mut self) {
        self.consumed = true;
        self.api_error = None;
        self.error = None;
    }

    fn size_in_bytes(&self) -> usize {
        0
    }

    fn high_watermark(&self) -> i64 {
        -1
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn set_initialized(&mut self) {
        self.initialized = true;
    }

    fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }
}

/// Default implementation of CompletedFetch for in-memory log records
pub struct DefaultCompletedFetch {
    table_bucket: TableBucket,
    log_record_batch: LogRecordsBatches,
    read_context: ReadContext,
    next_fetch_offset: i64,
    high_watermark: i64,
    size_in_bytes: usize,
    consumed: bool,
    initialized: bool,
    records_read: usize,
    current_record_iterator: Option<LogRecordIterator>,
    current_record_batch: Option<LogRecordBatch>,
    last_record: Option<ScanRecord>,
    cached_record_error: Option<String>,
    corrupt_last_record: bool,
}

impl DefaultCompletedFetch {
    pub fn new(
        table_bucket: TableBucket,
        log_record_batch: LogRecordsBatches,
        size_in_bytes: usize,
        read_context: ReadContext,
        fetch_offset: i64,
        high_watermark: i64,
    ) -> Result<Self> {
        Ok(Self {
            table_bucket,
            log_record_batch,
            read_context,
            next_fetch_offset: fetch_offset,
            high_watermark,
            size_in_bytes,
            consumed: false,
            initialized: false,
            records_read: 0,
            current_record_iterator: None,
            current_record_batch: None,
            last_record: None,
            cached_record_error: None,
            corrupt_last_record: false,
        })
    }

    /// Get the next fetched record, handling batch iteration and record skipping
    fn next_fetched_record(&mut self) -> Result<Option<ScanRecord>> {
        loop {
            if let Some(record) = self
                .current_record_iterator
                .as_mut()
                .and_then(Iterator::next)
            {
                if record.offset() >= self.next_fetch_offset {
                    return Ok(Some(record));
                }
            } else if let Some(batch) = self.log_record_batch.next() {
                self.current_record_iterator = Some(batch.records(&self.read_context)?);
                self.current_record_batch = Some(batch);
            } else {
                if let Some(batch) = self.current_record_batch.take() {
                    self.next_fetch_offset = batch.next_log_offset();
                }
                self.drain();
                return Ok(None);
            }
        }
    }

    fn fetch_error(&self) -> Error {
        let mut message = format!(
            "Received exception when fetching the next record from {table_bucket}. If needed, please back to past the record to continue scanning.",
            table_bucket = self.table_bucket
        );
        if let Some(cause) = self.cached_record_error.as_deref() {
            message.push_str(&format!(" Cause: {cause}"));
        }
        Error::UnexpectedError {
            message,
            source: None,
        }
    }

    /// Get the next batch directly without row iteration
    fn next_fetched_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let Some(log_batch) = self.log_record_batch.next() else {
                self.drain();
                return Ok(None);
            };

            let mut record_batch = log_batch.record_batch(&self.read_context)?;

            // Skip empty batches
            if record_batch.num_rows() == 0 {
                continue;
            }

            // Truncate batch
            let base_offset = log_batch.base_log_offset();
            if self.next_fetch_offset > base_offset {
                let skip_count = (self.next_fetch_offset - base_offset) as usize;
                if skip_count >= record_batch.num_rows() {
                    continue;
                }
                // Slice the batch to skip the first skip_count rows
                record_batch = record_batch.slice(skip_count, record_batch.num_rows() - skip_count);
            }

            self.next_fetch_offset = log_batch.next_log_offset();
            self.records_read += record_batch.num_rows();
            return Ok(Some(record_batch));
        }
    }
}

impl CompletedFetch for DefaultCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn api_error(&self) -> Option<&ApiError> {
        None
    }

    fn take_error(&mut self) -> Option<Error> {
        None
    }

    fn fetch_records(&mut self, max_records: usize) -> Result<Vec<ScanRecord>> {
        if self.corrupt_last_record {
            return Err(self.fetch_error());
        }

        if self.consumed {
            return Ok(Vec::new());
        }

        let mut scan_records = Vec::new();

        for _ in 0..max_records {
            if self.cached_record_error.is_none() {
                self.corrupt_last_record = true;
                match self.next_fetched_record() {
                    Ok(Some(record)) => {
                        self.corrupt_last_record = false;
                        self.last_record = Some(record);
                    }
                    Ok(None) => {
                        self.corrupt_last_record = false;
                        self.last_record = None;
                    }
                    Err(e) => {
                        self.cached_record_error = Some(e.to_string());
                    }
                }
            }

            let Some(record) = self.last_record.take() else {
                break;
            };

            self.next_fetch_offset = record.offset() + 1;
            self.records_read += 1;
            scan_records.push(record);
        }

        if self.cached_record_error.is_some() && scan_records.is_empty() {
            return Err(self.fetch_error());
        }

        Ok(scan_records)
    }

    fn fetch_batches(&mut self, max_batches: usize) -> Result<Vec<RecordBatch>> {
        if self.consumed {
            return Ok(Vec::new());
        }

        let mut batches = Vec::with_capacity(max_batches.min(16));

        for _ in 0..max_batches {
            match self.next_fetched_batch()? {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }

        Ok(batches)
    }

    fn is_consumed(&self) -> bool {
        self.consumed
    }

    fn records_read(&self) -> usize {
        self.records_read
    }

    fn drain(&mut self) {
        self.consumed = true;
        self.cached_record_error = None;
        self.corrupt_last_record = false;
        self.last_record = None;
    }

    fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn set_initialized(&mut self) {
        self.initialized = true;
    }

    fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{DataField, DataTypes, TablePath};
    use crate::record::{MemoryLogRecordsArrowBuilder, to_arrow_schema};
    use crate::row::GenericRow;
    use std::sync::Arc;
    use std::time::Duration;

    struct ErrorPendingFetch {
        table_bucket: TableBucket,
    }

    impl PendingFetch for ErrorPendingFetch {
        fn table_bucket(&self) -> &TableBucket {
            &self.table_bucket
        }

        fn is_completed(&self) -> bool {
            true
        }

        fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
            Err(Error::UnexpectedError {
                message: "pending fetch failure".to_string(),
                source: None,
            })
        }
    }

    #[tokio::test]
    async fn await_not_empty_returns_wakeup_error() {
        let buffer = LogFetchBuffer::new();
        buffer.wakeup();

        let result = buffer.await_not_empty(Duration::from_millis(10)).await;
        assert!(matches!(result, Err(Error::WakeupError { .. })));
    }

    #[tokio::test]
    async fn await_not_empty_returns_pending_error() {
        let buffer = LogFetchBuffer::new();
        let table_bucket = TableBucket::new(1, 0);
        buffer.pend(Box::new(ErrorPendingFetch {
            table_bucket: table_bucket.clone(),
        }));
        buffer.try_complete(&table_bucket);

        let result = buffer.await_not_empty(Duration::from_millis(10)).await;
        assert!(matches!(result, Ok(true)));

        let mut completed = buffer.poll().expect("completed fetch");
        assert!(completed.take_error().is_some());
    }

    #[test]
    fn default_completed_fetch_reads_records() -> Result<()> {
        let row_type = DataTypes::row(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
        );

        let mut row = GenericRow::new();
        row.set_field(0, 1_i32);
        row.set_field(1, "alice");
        let record = WriteRecord::new(table_path, row);
        builder.append(&record)?;

        let data = builder.build()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let read_context = ReadContext::new(to_arrow_schema(&row_type), false);
        let mut fetch = DefaultCompletedFetch::new(
            TableBucket::new(1, 0),
            log_records,
            data.len(),
            read_context,
            0,
            0,
        )?;

        let records = fetch.fetch_records(10)?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset(), 0);

        let empty = fetch.fetch_records(10)?;
        assert!(empty.is_empty());

        Ok(())
    }
}
