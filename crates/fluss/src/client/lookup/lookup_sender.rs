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

//! Lookup sender that processes batched lookup requests.
//!
//! The sender runs as a background task, draining lookups from the queue,
//! grouping them by destination server, and sending batched requests.

use super::{LookupQuery, LookupQueue};
use crate::client::metadata::Metadata;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::LookupResponse;
use crate::rpc::message::LookupRequest;
use crate::{BucketId, PartitionId, TableId};
use bytes::Bytes;
use log::{debug, error, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, watch};

/// Server ID type alias for clarity.
type ServerId = i32;

/// Batches grouped by table bucket, keyed by server.
type BatchesByLeader = HashMap<ServerId, HashMap<TableBucket, LookupBatch>>;

/// Result of grouping lookups by leader.
struct GroupByLeaderResult {
    /// Lookup batches grouped by leader server.
    batches_by_leader: BatchesByLeader,
    /// Tables with unknown leaders that need metadata refresh.
    unknown_leader_tables: HashSet<TablePath>,
    /// Partition IDs with unknown leaders.
    unknown_leader_partition_ids: HashSet<PartitionId>,
}

/// Lookup sender that batches and sends lookup requests.
pub struct LookupSender {
    /// Metadata for leader lookup
    metadata: Arc<Metadata>,
    /// The lookup queue to drain from
    queue: LookupQueue,
    /// Channel to re-enqueue failed lookups
    re_enqueue_tx: mpsc::UnboundedSender<LookupQuery>,
    /// Semaphore to limit in-flight requests
    inflight_semaphore: Arc<Semaphore>,
    /// Maximum number of retries
    max_retries: i32,
    /// Whether the sender is running
    running: AtomicBool,
    /// Whether to force close (abandon pending lookups)
    force_close: AtomicBool,
    /// Shutdown signal receiver
    shutdown_rx: watch::Receiver<bool>,
}

/// A batch of lookups going to the same table bucket.
struct LookupBatch {
    table_bucket: TableBucket,
    lookups: Vec<LookupQuery>,
    keys: Vec<Bytes>,
}

impl LookupBatch {
    fn new(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            lookups: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn add_lookup(&mut self, lookup: LookupQuery) {
        self.keys.push(lookup.key().clone());
        self.lookups.push(lookup);
    }

    fn complete(&mut self, values: Vec<Option<Vec<u8>>>) {
        if values.len() != self.lookups.len() {
            let err_msg = format!(
                "The number of return values ({}) does not match the number of lookups ({})",
                values.len(),
                self.lookups.len()
            );
            for lookup in &mut self.lookups {
                lookup.complete(Err(Error::UnexpectedError {
                    message: err_msg.clone(),
                    source: None,
                }));
            }
            return;
        }

        for (lookup, value) in self.lookups.iter_mut().zip(values.into_iter()) {
            lookup.complete(Ok(value));
        }
    }

    fn complete_exceptionally(&mut self, error_msg: &str) {
        for lookup in &mut self.lookups {
            lookup.complete(Err(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            }));
        }
    }
}

impl LookupSender {
    /// Creates a new lookup sender.
    pub fn new(
        metadata: Arc<Metadata>,
        queue: LookupQueue,
        re_enqueue_tx: mpsc::UnboundedSender<LookupQuery>,
        max_inflight_requests: usize,
        max_retries: i32,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            metadata,
            queue,
            re_enqueue_tx,
            inflight_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
            max_retries,
            running: AtomicBool::new(true),
            force_close: AtomicBool::new(false),
            shutdown_rx,
        }
    }

    /// Runs the sender loop.
    pub async fn run(&mut self) {
        debug!("Starting Fluss lookup sender");

        let mut shutdown_rx = self.shutdown_rx.clone();

        while self.running.load(Ordering::Acquire) {
            // Check for shutdown signal before entering select
            if *shutdown_rx.borrow() {
                debug!("Lookup sender received shutdown signal");
                self.initiate_close();
                break;
            }

            tokio::select! {
                biased;

                // Check shutdown signal
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("Lookup sender received shutdown signal during select");
                        self.initiate_close();
                    }
                }

                // Process lookups
                result = self.run_once(false) => {
                    if let Err(e) = result {
                        error!("Error in lookup sender: {}", e);
                    }
                }
            }
        }

        debug!("Beginning shutdown of lookup sender, sending remaining lookups");

        // Process remaining lookups during shutdown
        // TODO: Check the in flight request count in the accumulator.
        if !self.force_close.load(Ordering::Acquire) && self.queue.has_undrained() {
            if let Err(e) = self.run_once(true).await {
                error!("Error during lookup sender shutdown: {}", e);
            }
        }

        // TODO: If force close failed, add logic to abort incomplete lookup requests.
        debug!("Lookup sender shutdown complete");
    }

    /// Runs a single iteration of the sender loop.
    async fn run_once(&mut self, drain_all: bool) -> Result<()> {
        let lookups = if drain_all {
            self.queue.drain_all()
        } else {
            self.queue.drain().await
        };

        self.send_lookups(lookups).await
    }

    /// Groups and sends lookups to appropriate servers.
    async fn send_lookups(&self, lookups: Vec<LookupQuery>) -> Result<()> {
        if lookups.is_empty() {
            return Ok(());
        }

        // Group by leader
        let GroupByLeaderResult {
            batches_by_leader: lookup_batches,
            unknown_leader_tables,
            unknown_leader_partition_ids,
        } = self.group_by_leader(lookups);

        // Update metadata for tables with unknown leaders
        if !unknown_leader_tables.is_empty() {
            let table_paths_refs: HashSet<&TablePath> = unknown_leader_tables.iter().collect();
            let partition_ids: Vec<PartitionId> =
                unknown_leader_partition_ids.into_iter().collect();
            if let Err(e) = self
                .metadata
                .update_tables_metadata(&table_paths_refs, &HashSet::new(), partition_ids)
                .await
            {
                warn!("Failed to update metadata for unknown leader tables: {}", e);
            } else {
                debug!(
                    "Updated metadata due to unknown leader tables during lookup: {:?}",
                    unknown_leader_tables
                );
            }
        }

        // If no lookup batches, sleep a bit to avoid busy loop. This case will happen when there is
        // no leader for all the lookup request in queue.
        if lookup_batches.is_empty() && !self.queue.has_undrained() {
            // TODO: May use wait/notify mechanism to avoid active sleep, and use a dynamic sleep time based on the request waited time.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        // Send batches to each destination
        for (destination, batches) in lookup_batches {
            self.send_lookup_request(destination, batches).await;
        }

        Ok(())
    }

    /// Groups lookups by leader server.
    fn group_by_leader(&self, lookups: Vec<LookupQuery>) -> GroupByLeaderResult {
        let cluster = self.metadata.get_cluster();
        let mut batches_by_leader: BatchesByLeader = HashMap::new();
        let mut unknown_leader_tables: HashSet<TablePath> = HashSet::new();
        let mut unknown_leader_partition_ids: HashSet<PartitionId> = HashSet::new();

        for lookup in lookups {
            let table_bucket = lookup.table_bucket().clone();

            let leader = match cluster.leader_for(&table_bucket) {
                Some(leader) => leader.id(),
                None => {
                    warn!(
                        "No leader found for table bucket {} during lookup",
                        table_bucket
                    );
                    // Collect tables with unknown leaders for metadata update
                    unknown_leader_tables.insert(lookup.table_path().clone());
                    if let Some(partition_id) = table_bucket.partition_id() {
                        unknown_leader_partition_ids.insert(partition_id);
                    }
                    self.re_enqueue_lookup(lookup);
                    continue;
                }
            };

            batches_by_leader
                .entry(leader)
                .or_default()
                .entry(table_bucket.clone())
                .or_insert_with(|| LookupBatch::new(table_bucket))
                .add_lookup(lookup);
        }

        GroupByLeaderResult {
            batches_by_leader,
            unknown_leader_tables,
            unknown_leader_partition_ids,
        }
    }

    /// Sends lookup requests to a specific destination server.
    async fn send_lookup_request(
        &self,
        destination: i32,
        batches_by_bucket: HashMap<TableBucket, LookupBatch>,
    ) {
        // Group by table_id for request batching
        let mut batches_by_table: HashMap<TableId, Vec<LookupBatch>> = HashMap::new();
        for (table_bucket, batch) in batches_by_bucket {
            batches_by_table
                .entry(table_bucket.table_id())
                .or_default()
                .push(batch);
        }

        let cluster = self.metadata.get_cluster();
        let tablet_server = match cluster.get_tablet_server(destination) {
            Some(server) => server.clone(),
            None => {
                let err_msg = format!("Server {} is not found in metadata cache", destination);
                for batches in batches_by_table.into_values() {
                    for mut batch in batches {
                        self.handle_lookup_error(&err_msg, true, &mut batch);
                    }
                }
                return;
            }
        };

        let connection = match self.metadata.get_connection(&tablet_server).await {
            Ok(conn) => conn,
            Err(e) => {
                let err_msg = format!("Failed to get connection to server {}: {}", destination, e);
                for batches in batches_by_table.into_values() {
                    for mut batch in batches {
                        self.handle_lookup_error(&err_msg, true, &mut batch);
                    }
                }
                return;
            }
        };

        // Send requests for each table
        for (table_id, mut batches) in batches_by_table {
            // Build the request with all buckets for this table
            // Use std::mem::take to move keys instead of cloning to avoid deep copy overhead
            let mut all_keys_by_bucket: Vec<(BucketId, Option<PartitionId>, Vec<Bytes>)> =
                Vec::new();
            for batch in &mut batches {
                all_keys_by_bucket.push((
                    batch.table_bucket.bucket_id(),
                    batch.table_bucket.partition_id(),
                    std::mem::take(&mut batch.keys),
                ));
            }

            // Create lookup request for all buckets in this table
            let request = LookupRequest::new_batched(table_id, all_keys_by_bucket);

            // Acquire semaphore permit
            let _permit = match self.inflight_semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    error!("Semaphore closed during lookup");
                    for batch in &mut batches {
                        batch.complete_exceptionally("Lookup sender shutdown");
                    }
                    return;
                }
            };

            // Send request and handle response
            match connection.request(request).await {
                Ok(response) => {
                    self.handle_lookup_response(destination, response, &mut batches);
                }
                Err(e) => {
                    let err_msg = format!("Lookup request failed: {}", e);
                    let is_retriable = Self::is_retriable_error(&e);
                    for batch in &mut batches {
                        self.handle_lookup_error(&err_msg, is_retriable, batch);
                    }
                }
            }
        }
    }

    /// Checks if an error is retriable.
    fn is_retriable_error(error: &Error) -> bool {
        match error {
            Error::LeaderNotAvailable { .. } => true,
            Error::FlussAPIError { api_error } => {
                let fluss_error = FlussError::for_code(api_error.code);
                fluss_error.is_retriable()
            }
            _ => false,
        }
    }

    /// Handles the lookup response.
    fn handle_lookup_response(
        &self,
        destination: i32,
        response: LookupResponse,
        batches: &mut [LookupBatch],
    ) {
        // Create a map from bucket_id to batch index for quick lookup
        let bucket_to_index: HashMap<BucketId, usize> = batches
            .iter()
            .enumerate()
            .map(|(idx, batch)| (batch.table_bucket.bucket_id(), idx))
            .collect();

        // Track which batches have been processed
        let mut processed_batches = vec![false; batches.len()];

        for bucket_resp in response.buckets_resp {
            let bucket_id = bucket_resp.bucket_id;
            if let Some(&batch_idx) = bucket_to_index.get(&bucket_id) {
                processed_batches[batch_idx] = true;
                let batch = &mut batches[batch_idx];

                // Check for errors
                if let Some(error_code) = bucket_resp.error_code {
                    let fluss_error = FlussError::for_code(error_code);
                    if fluss_error != FlussError::None {
                        let err_msg = format!(
                            "Lookup error for bucket {}: code={}, message={}",
                            bucket_id,
                            error_code,
                            bucket_resp.error_message.unwrap_or_default()
                        );
                        let is_retriable = fluss_error.is_retriable();
                        self.handle_lookup_error(&err_msg, is_retriable, batch);
                        continue;
                    }
                }

                // Extract values
                let values: Vec<Option<Vec<u8>>> = bucket_resp
                    .values
                    .into_iter()
                    .map(|pb_value| pb_value.values)
                    .collect();

                batch.complete(values);
            } else {
                error!(
                    "Received response for unknown bucket {} from server {}",
                    bucket_id, destination
                );
            }
        }

        // Handle any batches that were not included in the response
        for (idx, processed) in processed_batches.iter().enumerate() {
            if !processed {
                let batch = &mut batches[idx];
                // If the batch has lookups that haven't been processed, retry them
                if !batch.lookups.is_empty() {
                    let err_msg = format!(
                        "Bucket {} response missing from server {}",
                        batch.table_bucket.bucket_id(),
                        destination
                    );
                    // Treat missing bucket response as retriable
                    self.handle_lookup_error(&err_msg, true, batch);
                }
            }
        }
    }

    /// Handles lookup errors with retry logic.
    fn handle_lookup_error(&self, error_msg: &str, is_retriable: bool, batch: &mut LookupBatch) {
        let mut lookups_to_retry = Vec::new();
        let mut lookups_to_complete = Vec::new();

        for lookup in batch.lookups.drain(..) {
            if is_retriable && lookup.retries() < self.max_retries && !lookup.is_done() {
                warn!(
                    "Lookup error for bucket {}, retrying ({} attempts left): {}",
                    batch.table_bucket,
                    self.max_retries - lookup.retries(),
                    error_msg
                );
                lookups_to_retry.push(lookup);
            } else {
                lookups_to_complete.push(lookup);
            }
        }

        // Re-enqueue retriable lookups
        for lookup in lookups_to_retry {
            lookup.increment_retries();
            self.re_enqueue_lookup(lookup);
        }

        // Complete non-retriable lookups with error
        for mut lookup in lookups_to_complete {
            warn!(
                "Lookup failed for bucket {}: {}",
                batch.table_bucket, error_msg
            );
            lookup.complete(Err(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            }));
        }
    }

    /// Re-enqueues a lookup for retry.
    fn re_enqueue_lookup(&self, lookup: LookupQuery) {
        if let Err(e) = self.re_enqueue_tx.send(lookup) {
            // Ensure the caller does not hang by completing the lookup with an error.
            error!("Failed to re-enqueue lookup: {}", e);
            let mut failed_lookup = e.0;
            failed_lookup.complete(Err(Error::UnexpectedError {
                message: "Failed to re-enqueue lookup: channel closed".to_string(),
                source: None,
            }));
        }
    }

    /// Initiates graceful shutdown of the sender.
    pub fn initiate_close(&mut self) {
        self.running.store(false, Ordering::Release);
    }

    /// Forces immediate shutdown, abandoning pending lookups.
    #[allow(dead_code)]
    pub fn force_close(&mut self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }
}
