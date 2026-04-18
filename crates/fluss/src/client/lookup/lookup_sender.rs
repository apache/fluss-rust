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

use super::{LookupQuery, LookupQueue, PrefixLookupQuery, PrimaryLookupQuery};
use crate::client::metadata::Metadata;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::{LookupResponse, PrefixLookupResponse};
use crate::rpc::ServerConnection;
use crate::rpc::message::{LookupRequest, PrefixLookupRequest};
use crate::{BucketId, PartitionId, TableId};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, error, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

type ServerId = i32;

type PrimaryBatchesByLeader = HashMap<ServerId, HashMap<TableBucket, PrimaryLookupBatch>>;
type PrefixBatchesByLeader = HashMap<ServerId, HashMap<TableBucket, PrefixLookupBatch>>;

struct GroupByLeaderResult {
    primary: PrimaryBatchesByLeader,
    prefix: PrefixBatchesByLeader,
    unknown_leader_tables: HashSet<TablePath>,
    unknown_leader_partition_ids: HashSet<PartitionId>,
}

impl GroupByLeaderResult {
    fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.prefix.is_empty()
    }
}

trait LookupQueryExt: Sized + Into<LookupQuery> {
    fn retries(&self) -> i32;
    fn increment_retries(&mut self);
    fn is_done(&self) -> bool;
    fn complete_with_error(&mut self, error: Error);
}

impl LookupQueryExt for PrimaryLookupQuery {
    fn retries(&self) -> i32 {
        PrimaryLookupQuery::retries(self)
    }
    fn increment_retries(&mut self) {
        PrimaryLookupQuery::increment_retries(self)
    }
    fn is_done(&self) -> bool {
        PrimaryLookupQuery::is_done(self)
    }
    fn complete_with_error(&mut self, error: Error) {
        PrimaryLookupQuery::complete_with_error(self, error)
    }
}

impl LookupQueryExt for PrefixLookupQuery {
    fn retries(&self) -> i32 {
        PrefixLookupQuery::retries(self)
    }
    fn increment_retries(&mut self) {
        PrefixLookupQuery::increment_retries(self)
    }
    fn is_done(&self) -> bool {
        PrefixLookupQuery::is_done(self)
    }
    fn complete_with_error(&mut self, error: Error) {
        PrefixLookupQuery::complete_with_error(self, error)
    }
}

trait LookupBatchExt {
    type Query: LookupQueryExt;

    fn table_bucket(&self) -> &TableBucket;
    fn drain_lookups(&mut self) -> std::vec::Drain<'_, Self::Query>;
    fn complete_exceptionally(&mut self, error_msg: &str);
    fn take_keys(&mut self) -> Vec<Bytes>;

    fn bucket_coords(&self) -> (BucketId, Option<PartitionId>) {
        let tb = self.table_bucket();
        (tb.bucket_id(), tb.partition_id())
    }

    fn keys_tuple(&mut self) -> (BucketId, Option<PartitionId>, Vec<Bytes>) {
        let (bucket, partition) = self.bucket_coords();
        (bucket, partition, self.take_keys())
    }
}

pub struct LookupSender {
    metadata: Arc<Metadata>,
    queue: LookupQueue,
    re_enqueue_tx: mpsc::UnboundedSender<LookupQuery>,
    inflight_semaphore: Arc<Semaphore>,
    max_retries: i32,
    running: AtomicBool,
    force_close: AtomicBool,
    shutdown_rx: watch::Receiver<bool>,
}

struct PrimaryLookupBatch {
    table_bucket: TableBucket,
    lookups: Vec<PrimaryLookupQuery>,
    keys: Vec<Bytes>,
}

impl PrimaryLookupBatch {
    fn new(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            lookups: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn add_lookup(&mut self, lookup: PrimaryLookupQuery) {
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
                lookup.complete_with_error(Error::UnexpectedError {
                    message: err_msg.clone(),
                    source: None,
                });
            }
            return;
        }

        for (lookup, value) in self.lookups.iter_mut().zip(values.into_iter()) {
            lookup.complete(Ok(value));
        }
    }
}

impl LookupBatchExt for PrimaryLookupBatch {
    type Query = PrimaryLookupQuery;

    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }
    fn drain_lookups(&mut self) -> std::vec::Drain<'_, Self::Query> {
        self.lookups.drain(..)
    }
    fn complete_exceptionally(&mut self, error_msg: &str) {
        for lookup in &mut self.lookups {
            lookup.complete_with_error(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            });
        }
    }
    fn take_keys(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.keys)
    }
}

struct PrefixLookupBatch {
    table_bucket: TableBucket,
    lookups: Vec<PrefixLookupQuery>,
    keys: Vec<Bytes>,
}

impl PrefixLookupBatch {
    fn new(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            lookups: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn add_lookup(&mut self, lookup: PrefixLookupQuery) {
        self.keys.push(lookup.key().clone());
        self.lookups.push(lookup);
    }

    fn complete(&mut self, value_lists: Vec<Vec<Vec<u8>>>) {
        if value_lists.len() != self.lookups.len() {
            let err_msg = format!(
                "The number of return value lists ({}) does not match the number of prefix lookups ({})",
                value_lists.len(),
                self.lookups.len()
            );
            for lookup in &mut self.lookups {
                lookup.complete_with_error(Error::UnexpectedError {
                    message: err_msg.clone(),
                    source: None,
                });
            }
            return;
        }

        for (lookup, values) in self.lookups.iter_mut().zip(value_lists.into_iter()) {
            lookup.complete(Ok(values));
        }
    }
}

impl LookupBatchExt for PrefixLookupBatch {
    type Query = PrefixLookupQuery;

    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }
    fn drain_lookups(&mut self) -> std::vec::Drain<'_, Self::Query> {
        self.lookups.drain(..)
    }
    fn complete_exceptionally(&mut self, error_msg: &str) {
        for lookup in &mut self.lookups {
            lookup.complete_with_error(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            });
        }
    }
    fn take_keys(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.keys)
    }
}

impl LookupSender {
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

    pub async fn run(&mut self) {
        debug!("Starting Fluss lookup sender");

        let mut shutdown_rx = self.shutdown_rx.clone();

        while self.running.load(Ordering::Acquire) {
            if *shutdown_rx.borrow() {
                debug!("Lookup sender received shutdown signal");
                self.initiate_close();
                break;
            }

            tokio::select! {
                biased;

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("Lookup sender received shutdown signal during select");
                        self.initiate_close();
                    }
                }

                result = self.run_once(false) => {
                    if let Err(e) = result {
                        error!("Error in lookup sender: {}", e);
                    }
                }
            }
        }

        debug!("Beginning shutdown of lookup sender, sending remaining lookups");

        // TODO: Check the in-flight request count in the accumulator.
        if !self.force_close.load(Ordering::Acquire) && self.queue.has_undrained() {
            if let Err(e) = self.run_once(true).await {
                error!("Error during lookup sender shutdown: {}", e);
            }
        }

        // TODO: If force close failed, add logic to abort incomplete lookup requests.
        debug!("Lookup sender shutdown complete");
    }

    async fn run_once(&mut self, drain_all: bool) -> Result<()> {
        let lookups = if drain_all {
            self.queue.drain_all()
        } else {
            self.queue.drain().await
        };

        self.send_lookups(lookups).await
    }

    async fn send_lookups(&self, lookups: Vec<LookupQuery>) -> Result<()> {
        if lookups.is_empty() {
            return Ok(());
        }

        let group = self.group_by_leader(lookups);

        if !group.unknown_leader_tables.is_empty() {
            let table_paths_refs: HashSet<&TablePath> = group.unknown_leader_tables.iter().collect();
            let partition_ids: Vec<PartitionId> =
                group.unknown_leader_partition_ids.iter().copied().collect();
            if let Err(e) = self
                .metadata
                .update_tables_metadata(&table_paths_refs, &HashSet::new(), partition_ids)
                .await
            {
                warn!("Failed to update metadata for unknown leader tables: {}", e);
            } else {
                debug!(
                    "Updated metadata due to unknown leader tables during lookup: {:?}",
                    group.unknown_leader_tables
                );
            }
        }

        if group.is_empty() && !self.queue.has_undrained() {
            let mut cluster_rx = self.metadata.subscribe_cluster_changes();
            tokio::select! {
                _ = cluster_rx.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            return Ok(());
        }

        let primary_fut = async {
            let mut pending = FuturesUnordered::new();
            for (server, batches) in group.primary {
                pending.push(self.send_primary_request(server, batches));
            }
            while pending.next().await.is_some() {}
        };
        let prefix_fut = async {
            let mut pending = FuturesUnordered::new();
            for (server, batches) in group.prefix {
                pending.push(self.send_prefix_request(server, batches));
            }
            while pending.next().await.is_some() {}
        };
        tokio::join!(primary_fut, prefix_fut);

        Ok(())
    }

    fn group_by_leader(&self, lookups: Vec<LookupQuery>) -> GroupByLeaderResult {
        let cluster = self.metadata.get_cluster();
        let mut primary: PrimaryBatchesByLeader = HashMap::new();
        let mut prefix: PrefixBatchesByLeader = HashMap::new();
        let mut unknown_leader_tables: HashSet<TablePath> = HashSet::new();
        let mut unknown_leader_partition_ids: HashSet<PartitionId> = HashSet::new();

        for query in lookups {
            let table_bucket = query.table_bucket().clone();

            let leader = match cluster.leader_for(&table_bucket) {
                Some(leader) => leader.id(),
                None => {
                    warn!(
                        "No leader found for table bucket {} during lookup",
                        table_bucket
                    );
                    unknown_leader_tables.insert(query.table_path().clone());
                    if let Some(partition_id) = table_bucket.partition_id() {
                        unknown_leader_partition_ids.insert(partition_id);
                    }
                    self.re_enqueue_lookup(query);
                    continue;
                }
            };

            match query {
                LookupQuery::Primary(q) => {
                    primary
                        .entry(leader)
                        .or_default()
                        .entry(table_bucket.clone())
                        .or_insert_with(|| PrimaryLookupBatch::new(table_bucket))
                        .add_lookup(q);
                }
                LookupQuery::Prefix(q) => {
                    prefix
                        .entry(leader)
                        .or_default()
                        .entry(table_bucket.clone())
                        .or_insert_with(|| PrefixLookupBatch::new(table_bucket))
                        .add_lookup(q);
                }
            }
        }

        GroupByLeaderResult {
            primary,
            prefix,
            unknown_leader_tables,
            unknown_leader_partition_ids,
        }
    }

    async fn send_primary_request(
        &self,
        destination: ServerId,
        batches_by_bucket: HashMap<TableBucket, PrimaryLookupBatch>,
    ) {
        let mut batches_by_table = group_by_table(batches_by_bucket);
        let connection = match self.connect_or_fail(destination, &mut batches_by_table).await {
            Some(conn) => conn,
            None => return,
        };

        let mut pending = FuturesUnordered::new();
        for (table_id, mut batches) in batches_by_table {
            let keys_by_bucket: Vec<_> = batches.iter_mut().map(|b| b.keys_tuple()).collect();
            let request = LookupRequest::new_batched(table_id, keys_by_bucket);
            pending.push(self.send_single_table_primary_lookup(
                table_id,
                destination,
                connection.clone(),
                request,
                batches,
            ));
        }
        while pending.next().await.is_some() {}
    }

    async fn send_prefix_request(
        &self,
        destination: ServerId,
        batches_by_bucket: HashMap<TableBucket, PrefixLookupBatch>,
    ) {
        let mut batches_by_table = group_by_table(batches_by_bucket);
        let connection = match self.connect_or_fail(destination, &mut batches_by_table).await {
            Some(conn) => conn,
            None => return,
        };

        let mut pending = FuturesUnordered::new();
        for (table_id, mut batches) in batches_by_table {
            let keys_by_bucket: Vec<_> = batches.iter_mut().map(|b| b.keys_tuple()).collect();
            let request = PrefixLookupRequest::new_batched(table_id, keys_by_bucket);
            pending.push(self.send_single_table_prefix_lookup(
                table_id,
                destination,
                connection.clone(),
                request,
                batches,
            ));
        }
        while pending.next().await.is_some() {}
    }

    async fn connect_or_fail<B: LookupBatchExt>(
        &self,
        destination: ServerId,
        batches_by_table: &mut HashMap<TableId, Vec<B>>,
    ) -> Option<ServerConnection> {
        let cluster = self.metadata.get_cluster();
        let tablet_server = match cluster.get_tablet_server(destination) {
            Some(server) => server.clone(),
            None => {
                let err_msg = format!("Server {} is not found in metadata cache", destination);
                self.fail_all_batches(&err_msg, true, batches_by_table);
                return None;
            }
        };

        match self.metadata.get_connection(&tablet_server).await {
            Ok(conn) => Some(conn),
            Err(e) => {
                let err_msg = format!("Failed to get connection to server {}: {}", destination, e);
                self.fail_all_batches(&err_msg, true, batches_by_table);
                None
            }
        }
    }

    fn fail_all_batches<B: LookupBatchExt>(
        &self,
        err_msg: &str,
        is_retriable: bool,
        batches_by_table: &mut HashMap<TableId, Vec<B>>,
    ) {
        for batches in batches_by_table.values_mut() {
            for batch in batches.iter_mut() {
                self.handle_batch_error(err_msg, is_retriable, batch);
            }
        }
    }

    async fn send_single_table_primary_lookup(
        &self,
        table_id: TableId,
        destination: ServerId,
        connection: ServerConnection,
        request: LookupRequest,
        mut batches: Vec<PrimaryLookupBatch>,
    ) {
        let _permit = match self.acquire_inflight_permit(&mut batches).await {
            Some(p) => p,
            None => return,
        };

        match connection.request(request).await {
            Ok(response) => {
                self.handle_lookup_response(table_id, destination, response, &mut batches);
            }
            Err(e) => {
                let err_msg = format!("Lookup request failed: {}", e);
                let is_retriable = e.is_retriable();
                for batch in &mut batches {
                    self.handle_batch_error(&err_msg, is_retriable, batch);
                }
            }
        }
    }

    async fn send_single_table_prefix_lookup(
        &self,
        table_id: TableId,
        destination: ServerId,
        connection: ServerConnection,
        request: PrefixLookupRequest,
        mut batches: Vec<PrefixLookupBatch>,
    ) {
        let _permit = match self.acquire_inflight_permit(&mut batches).await {
            Some(p) => p,
            None => return,
        };

        match connection.request(request).await {
            Ok(response) => {
                self.handle_prefix_lookup_response(table_id, destination, response, &mut batches);
            }
            Err(e) => {
                let err_msg = format!("Prefix lookup request failed: {}", e);
                let is_retriable = e.is_retriable();
                for batch in &mut batches {
                    self.handle_batch_error(&err_msg, is_retriable, batch);
                }
            }
        }
    }

    async fn acquire_inflight_permit<B: LookupBatchExt>(
        &self,
        batches: &mut [B],
    ) -> Option<OwnedSemaphorePermit> {
        match self.inflight_semaphore.clone().acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                error!("Semaphore closed during lookup");
                for batch in batches.iter_mut() {
                    batch.complete_exceptionally("Lookup sender shutdown");
                }
                None
            }
        }
    }

    fn handle_lookup_response(
        &self,
        table_id: TableId,
        destination: ServerId,
        response: LookupResponse,
        batches: &mut [PrimaryLookupBatch],
    ) {
        let bucket_to_index = build_bucket_index(batches);
        let mut processed = vec![false; batches.len()];

        for bucket_resp in response.buckets_resp {
            let table_bucket = TableBucket::new_with_partition(
                table_id,
                bucket_resp.partition_id,
                bucket_resp.bucket_id,
            );
            let Some(&idx) = bucket_to_index.get(&table_bucket) else {
                error!(
                    "Received response for unknown bucket {} from server {}",
                    table_bucket, destination
                );
                continue;
            };
            processed[idx] = true;
            let batch = &mut batches[idx];

            if let Some(err) = extract_bucket_error(
                bucket_resp.error_code,
                bucket_resp.error_message,
                &table_bucket,
                "Lookup",
            ) {
                self.handle_batch_error(&err.message, err.is_retriable, batch);
                continue;
            }

            let values: Vec<Option<Vec<u8>>> = bucket_resp
                .values
                .into_iter()
                .map(|pb_value| pb_value.values)
                .collect();
            batch.complete(values);
        }

        self.fail_unprocessed_batches(&processed, batches, destination, "response");
    }

    fn handle_prefix_lookup_response(
        &self,
        table_id: TableId,
        destination: ServerId,
        response: PrefixLookupResponse,
        batches: &mut [PrefixLookupBatch],
    ) {
        let bucket_to_index = build_bucket_index(batches);
        let mut processed = vec![false; batches.len()];

        for bucket_resp in response.buckets_resp {
            let table_bucket = TableBucket::new_with_partition(
                table_id,
                bucket_resp.partition_id,
                bucket_resp.bucket_id,
            );
            let Some(&idx) = bucket_to_index.get(&table_bucket) else {
                error!(
                    "Received prefix response for unknown bucket {} from server {}",
                    table_bucket, destination
                );
                continue;
            };
            processed[idx] = true;
            let batch = &mut batches[idx];

            if let Some(err) = extract_bucket_error(
                bucket_resp.error_code,
                bucket_resp.error_message,
                &table_bucket,
                "Prefix lookup",
            ) {
                self.handle_batch_error(&err.message, err.is_retriable, batch);
                continue;
            }

            let value_lists: Vec<Vec<Vec<u8>>> = bucket_resp
                .value_lists
                .into_iter()
                .map(|pb_list| pb_list.values)
                .collect();
            batch.complete(value_lists);
        }

        self.fail_unprocessed_batches(&processed, batches, destination, "prefix response");
    }

    fn fail_unprocessed_batches<B: LookupBatchExt>(
        &self,
        processed: &[bool],
        batches: &mut [B],
        destination: ServerId,
        what: &str,
    ) {
        for (idx, was_processed) in processed.iter().enumerate() {
            if !was_processed {
                let batch = &mut batches[idx];
                let err_msg = format!(
                    "Bucket {} {} missing from server {}",
                    batch.table_bucket().bucket_id(),
                    what,
                    destination
                );
                self.handle_batch_error(&err_msg, true, batch);
            }
        }
    }

    fn handle_batch_error<B: LookupBatchExt>(
        &self,
        error_msg: &str,
        is_retriable: bool,
        batch: &mut B,
    ) {
        let mut retried = 0usize;
        let mut failed = 0usize;
        let table_bucket = batch.table_bucket().clone();

        for mut lookup in batch.drain_lookups() {
            if is_retriable && lookup.retries() < self.max_retries && !lookup.is_done() {
                lookup.increment_retries();
                self.re_enqueue_lookup(lookup.into());
                retried += 1;
            } else {
                lookup.complete_with_error(Error::UnexpectedError {
                    message: error_msg.to_string(),
                    source: None,
                });
                failed += 1;
            }
        }

        if retried > 0 {
            warn!(
                "Lookup error for bucket {}, retrying {} lookups: {}",
                table_bucket, retried, error_msg
            );
        }
        if failed > 0 {
            warn!(
                "Lookup failed for bucket {} ({} lookups): {}",
                table_bucket, failed, error_msg
            );
        }
    }

    fn re_enqueue_lookup(&self, lookup: LookupQuery) {
        if let Err(e) = self.re_enqueue_tx.send(lookup) {
            error!("Failed to re-enqueue lookup: {}", e);
            let mut failed_lookup = e.0;
            failed_lookup.complete_with_error(Error::UnexpectedError {
                message: "Failed to re-enqueue lookup: channel closed".to_string(),
                source: None,
            });
        }
    }

    pub fn initiate_close(&mut self) {
        self.running.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn force_close(&mut self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }
}

fn group_by_table<B: LookupBatchExt>(
    batches_by_bucket: HashMap<TableBucket, B>,
) -> HashMap<TableId, Vec<B>> {
    let mut out: HashMap<TableId, Vec<B>> = HashMap::new();
    for (table_bucket, batch) in batches_by_bucket {
        out.entry(table_bucket.table_id()).or_default().push(batch);
    }
    out
}

fn build_bucket_index<B: LookupBatchExt>(batches: &[B]) -> HashMap<TableBucket, usize> {
    batches
        .iter()
        .enumerate()
        .map(|(idx, batch)| (batch.table_bucket().clone(), idx))
        .collect()
}

struct BucketError {
    message: String,
    is_retriable: bool,
}

fn extract_bucket_error(
    error_code: Option<i32>,
    error_message: Option<String>,
    table_bucket: &TableBucket,
    op: &str,
) -> Option<BucketError> {
    let code = error_code?;
    let fluss_error = FlussError::for_code(code);
    if fluss_error == FlussError::None {
        return None;
    }
    Some(BucketError {
        message: format!(
            "{} error for bucket {}: code={}, message={}",
            op,
            table_bucket,
            code,
            error_message.unwrap_or_default()
        ),
        is_retriable: fluss_error.is_retriable(),
    })
}
