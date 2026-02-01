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

//! Lookup client that batches multiple lookups together for improved throughput.
//!
//! This client achieves parity with the Java client by:
//! - Queuing lookup operations instead of sending them immediately
//! - Batching multiple lookups to the same server/bucket
//! - Running a background sender task to process batches

use super::{LookupQuery, LookupQueue};
use crate::client::lookup::lookup_sender::LookupSender;
use crate::client::metadata::Metadata;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TablePath};
use bytes::Bytes;
use log::{debug, error};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A client that lookups values from the server with batching support.
///
/// The lookup client uses a queue and background sender to batch multiple
/// lookup operations together, reducing network round trips and improving
/// throughput.
///
/// # Example
///
/// ```ignore
/// let lookup_client = LookupClient::new(config, metadata);
/// let result = lookup_client.lookup(table_path, table_bucket, key_bytes).await?;
/// ```
pub struct LookupClient {
    /// Channel to send lookup requests to the queue
    lookup_tx: mpsc::Sender<LookupQuery>,
    /// Handle to the sender task
    sender_handle: Option<JoinHandle<()>>,
    /// Shutdown signal sender
    shutdown_tx: mpsc::Sender<()>,
    /// Whether the client is closed
    closed: AtomicBool,
}

impl LookupClient {
    /// Creates a new lookup client.
    pub fn new(config: &Config, metadata: Arc<Metadata>) -> Self {
        // Extract configuration values
        let queue_size = config.lookup_queue_size;
        let max_batch_size = config.lookup_max_batch_size;
        let batch_timeout_ms = config.lookup_batch_timeout_ms;
        let max_inflight = config.lookup_max_inflight_requests;
        let max_retries = config.lookup_max_retries;

        // Create queue and channels
        let (queue, lookup_tx, re_enqueue_tx) =
            LookupQueue::new(queue_size, max_batch_size, batch_timeout_ms);

        // Create sender
        let mut sender =
            LookupSender::new(metadata, queue, re_enqueue_tx, max_inflight, max_retries);

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        // Spawn sender task
        let sender_handle = tokio::spawn(async move {
            tokio::select! {
                _ = sender.run() => {
                    debug!("Lookup sender completed");
                }
                _ = shutdown_rx.recv() => {
                    debug!("Lookup sender received shutdown signal");
                    sender.initiate_close();
                }
            }
        });

        Self {
            lookup_tx,
            sender_handle: Some(sender_handle),
            shutdown_tx,
            closed: AtomicBool::new(false),
        }
    }

    /// Looks up a value by its primary key.
    ///
    /// This method queues the lookup operation and returns a future that will
    /// complete when the server responds. Multiple lookups may be batched together
    /// for improved throughput.
    ///
    /// # Arguments
    /// * `table_path` - The table path
    /// * `table_bucket` - The table bucket
    /// * `key_bytes` - The encoded primary key bytes
    ///
    /// # Returns
    /// * `Ok(Some(bytes))` - The value bytes if found
    /// * `Ok(None)` - If the key was not found
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(
        &self,
        table_path: TablePath,
        table_bucket: TableBucket,
        key_bytes: Bytes,
    ) -> Result<Option<Vec<u8>>> {
        // Check if the client is closed
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::UnexpectedError {
                message: "Lookup client is closed".to_string(),
                source: None,
            });
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let query = LookupQuery::new(table_path, table_bucket, key_bytes, result_tx);

        // Send to queue
        self.lookup_tx
            .send(query)
            .await
            .map_err(|e| {
                let failed_query = e.0;
                error!(
                    "Failed to queue lookup: channel closed. table_path: {}, table_bucket: {:?}, key_len: {}",
                    failed_query.table_path(),
                    failed_query.table_bucket(),
                    failed_query.key().len()
                );
                Error::UnexpectedError {
                    message: "Failed to queue lookup: channel closed".to_string(),
                    source: None,
                }
            })?;

        // Wait for result
        result_rx.await.map_err(|_| Error::UnexpectedError {
            message: "Lookup result channel closed".to_string(),
            source: None,
        })?
    }

    /// Closes the lookup client gracefully.
    pub async fn close(mut self, timeout: Duration) {
        debug!("Closing lookup client");

        // Mark as closed to reject new lookups
        self.closed.store(true, Ordering::Release);

        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        // Wait for sender to complete with timeout
        if let Some(handle) = self.sender_handle.take() {
            debug!("Waiting for sender task to complete...");
            let abort_handle = handle.abort_handle();

            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    debug!("Lookup sender task completed gracefully.");
                }
                Ok(Err(join_error)) => {
                    error!("Lookup sender task panicked: {:?}", join_error);
                }
                Err(_elapsed) => {
                    error!("Lookup sender task did not complete within timeout. Forcing shutdown.");
                    abort_handle.abort();
                }
            }
        } else {
            debug!("Lookup client was already closed or never initialized properly.");
        }

        debug!("Lookup client closed");
    }
}

impl Drop for LookupClient {
    fn drop(&mut self) {
        // Abort the sender task on drop if it wasn't already consumed by close()
        if let Some(handle) = self.sender_handle.take() {
            handle.abort();
        }
    }
}
