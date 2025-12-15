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

use crate::error::{Error, Result};
use crate::metadata::TableBucket;
use crate::proto::PbFetchLogRespForBucket;
use crate::record::{LogRecordsBatchs, ReadContext, ScanRecord};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

/// Represents a completed fetch that can be consumed
pub trait CompletedFetch: Send + Sync {
    fn table_bucket(&self) -> &TableBucket;
    fn fetch_records(&mut self, max_records: usize) -> Result<Vec<ScanRecord>>;
    fn next_fetch_offset(&self) -> i64;
    fn is_consumed(&self) -> bool;
    fn drain(&mut self);
    fn size_in_bytes(&self) -> usize;
    fn high_watermark(&self) -> i64;
    fn is_initialized(&self) -> bool;
    fn set_initialized(&mut self);
    fn fetch_offset(&self) -> i64;
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
    
    /// Wait for the buffer to become non-empty, with timeout
    /// Returns true if data became available, false if timeout
    pub async fn await_not_empty(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        
        loop {
            // Check if buffer is not empty
            if !self.is_empty() {
                return true;
            }
            
            // Check if woken up
            if self.woken_up.swap(false, Ordering::Acquire) {
                return true;
            }
            
            // Check if timeout
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            
            // Wait for notification with remaining time
            let remaining = deadline - now;
            let notified = self.not_empty_notify.notified();
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {
                    return false; // Timeout
                }
                _ = notified => {
                    // Got notification, check again
                    continue;
                }
            }
        }
    }
    
    /// Wake up any waiting threads
    pub fn wakeup(&self) {
        self.woken_up.store(true, Ordering::Release);
        self.not_empty_notify.notify_waiters();
    }

    /// Add a pending fetch to the buffer
    pub fn pend(&self, pending_fetch: Box<dyn PendingFetch>) {
        let table_bucket = pending_fetch.table_bucket().clone();
        self.pending_fetches
            .lock()
            .entry(table_bucket)
            .or_insert_with(VecDeque::new)
            .push_back(pending_fetch);
    }

    /// Try to complete pending fetches in order, converting them to completed fetches
    pub fn try_complete(&self, table_bucket: &TableBucket) {
        let mut pending_map = self.pending_fetches.lock();
        if let Some(pendings) = pending_map.get_mut(table_bucket) {
            let mut has_completed = false;
            while let Some(front) = pendings.front() {
                if front.is_completed() {
                    if let Some(pending) = pendings.pop_front() {
                        match pending.to_completed_fetch() {
                            Ok(completed) => {
                                self.completed_fetches.lock().push_back(completed);
                                // Signal that buffer is not empty
                                self.not_empty_notify.notify_waiters();
                                has_completed = true;
                            }
                            Err(_) => {
                                // Skip failed fetches
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            if pendings.is_empty() {
                pending_map.remove(table_bucket);
            }
        }
    }

    /// Add a completed fetch to the buffer
    pub fn add(&self, completed_fetch: Box<dyn CompletedFetch>) {
        let table_bucket = completed_fetch.table_bucket().clone();
        let mut pending_map = self.pending_fetches.lock();
        let should_notify = if let Some(pendings) = pending_map.get_mut(&table_bucket) {
            if pendings.is_empty() {
                self.completed_fetches.lock().push_back(completed_fetch);
                true
            } else {
                // Convert to pending fetch wrapper
                let completed_pending = CompletedPendingFetch::new(completed_fetch);
                pendings.push_back(Box::new(completed_pending));
                false
            }
        } else {
            self.completed_fetches.lock().push_back(completed_fetch);
            true
        };
        
        // Signal that buffer is not empty if we added to completed_fetches
        if should_notify {
            self.not_empty_notify.notify_waiters();
        }
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

/// Pending fetch that waits for fetch log response
pub struct FetchPendingFetch {
    table_bucket: TableBucket,
    response: Arc<Mutex<Option<Result<PbFetchLogRespForBucket>>>>,
    read_context: ReadContext,
    fetch_offset: i64,
}

impl FetchPendingFetch {
    pub fn new(
        table_bucket: TableBucket,
        read_context: ReadContext,
        fetch_offset: i64,
    ) -> (Self, Arc<Mutex<Option<Result<PbFetchLogRespForBucket>>>>) {
        let response = Arc::new(Mutex::new(None));
        let pending = Self {
            table_bucket,
            response: Arc::clone(&response),
            read_context,
            fetch_offset,
        };
        (pending, response)
    }

    pub fn set_response(&self, response: Result<PbFetchLogRespForBucket>) {
        *self.response.lock() = Some(response);
    }
}

impl PendingFetch for FetchPendingFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn is_completed(&self) -> bool {
        self.response.lock().is_some()
    }

    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
        let response = self.response.lock().take().ok_or_else(|| {
            Error::Io(std::io::Error::other("Fetch response not available"))
        })??;
        let completed = DefaultCompletedFetch::new(
            self.table_bucket,
            &response,
            self.read_context,
            self.fetch_offset,
        )?;
        Ok(Box::new(completed))
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

/// Default implementation of CompletedFetch for in-memory log records
pub struct DefaultCompletedFetch {
    table_bucket: TableBucket,
    data: Vec<u8>,
    read_context: ReadContext,
    fetch_offset: i64, // The offset at which this fetch started
    next_fetch_offset: i64,
    high_watermark: i64,
    size_in_bytes: usize,
    consumed: bool,
    initialized: bool,
    // Pre-parsed records for efficient access
    records: Vec<ScanRecord>,
    current_index: usize,
}

impl DefaultCompletedFetch {
    pub fn new(
        table_bucket: TableBucket,
        fetch_response: &PbFetchLogRespForBucket,
        read_context: ReadContext,
        fetch_offset: i64,
    ) -> Result<Self> {
        let data = fetch_response.records.clone().unwrap_or_default();
        let size_in_bytes = data.len();
        let high_watermark = fetch_response.high_watermark.unwrap_or(-1);

        // Parse all records upfront
        let mut records = Vec::new();
        for log_record in &mut LogRecordsBatchs::new(&data) {
            let last_offset = log_record.last_log_offset();
            let batch_records = log_record.records(&read_context)?;
            for record in batch_records {
                records.push(record);
            }
            // Update next_fetch_offset based on the last batch
            let next_offset = last_offset + 1;
            // We'll update this when we actually consume records
        }

        // Set next_fetch_offset based on the last record if available
        let next_fetch_offset = if let Some(last_record) = records.last() {
            last_record.offset() + 1
        } else {
            fetch_offset
        };

        Ok(Self {
            table_bucket,
            data,
            read_context,
            fetch_offset,
            next_fetch_offset,
            high_watermark,
            size_in_bytes,
            consumed: false,
            initialized: false,
            records,
            current_index: 0,
        })
    }
}

impl CompletedFetch for DefaultCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn fetch_records(&mut self, max_records: usize) -> Result<Vec<ScanRecord>> {
        if self.consumed {
            return Ok(Vec::new());
        }

        let end_index = std::cmp::min(self.current_index + max_records, self.records.len());
        let records = self.records[self.current_index..end_index].to_vec();
        self.current_index = end_index;

        if self.current_index >= self.records.len() {
            self.consumed = true;
            // Update next_fetch_offset based on the last record
            if let Some(last_record) = self.records.last() {
                self.next_fetch_offset = last_record.offset() + 1;
            }
        } else if let Some(last_record) = records.last() {
            // Update next_fetch_offset as we consume records
            self.next_fetch_offset = last_record.offset() + 1;
        }

        Ok(records)
    }

    fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }

    fn is_consumed(&self) -> bool {
        self.consumed
    }

    fn drain(&mut self) {
        self.consumed = true;
        self.current_index = self.records.len();
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

    fn fetch_offset(&self) -> i64 {
        self.fetch_offset
    }
}
