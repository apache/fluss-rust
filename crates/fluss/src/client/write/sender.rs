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

use crate::client::broadcast;
use crate::client::metadata::Metadata;
use crate::client::write::batch::WriteBatch;
use crate::client::{ReadyWriteBatch, RecordAccumulator};
use crate::error::{FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::{
    PbProduceLogRespForBucket, PbPutKvRespForBucket, ProduceLogResponse, PutKvResponse,
};
use crate::rpc::ServerConnection;
use crate::rpc::message::{ProduceLogRequest, PutKvRequest};
use log::warn;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

#[allow(dead_code)]
pub struct Sender {
    running: bool,
    metadata: Arc<Metadata>,
    accumulator: Arc<RecordAccumulator>,
    in_flight_batches: Mutex<HashMap<TableBucket, Vec<i64>>>,
    max_request_size: i32,
    ack: i16,
    max_request_timeout_ms: i32,
    retries: i32,
}

impl Sender {
    pub fn new(
        metadata: Arc<Metadata>,
        accumulator: Arc<RecordAccumulator>,
        max_request_size: i32,
        max_request_timeout_ms: i32,
        ack: i16,
        retries: i32,
    ) -> Self {
        Self {
            running: true,
            metadata,
            accumulator,
            in_flight_batches: Default::default(),
            max_request_size,
            ack,
            max_request_timeout_ms,
            retries,
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            if !self.running {
                return Ok(());
            }
            self.run_once().await?;
        }
    }

    async fn run_once(&self) -> Result<()> {
        let cluster = self.metadata.get_cluster();
        let ready_check_result = self.accumulator.ready(&cluster).await;

        // Update metadata if needed
        if !ready_check_result.unknown_leader_tables.is_empty() {
            self.metadata
                .update_tables_metadata(&ready_check_result.unknown_leader_tables.iter().collect())
                .await?;
        }

        if ready_check_result.ready_nodes.is_empty() {
            tokio::time::sleep(Duration::from_millis(
                ready_check_result.next_ready_check_delay_ms as u64,
            ))
            .await;
            return Ok(());
        }

        let batches = self
            .accumulator
            .drain(
                cluster.clone(),
                &ready_check_result.ready_nodes,
                self.max_request_size,
            )
            .await?;

        if !batches.is_empty() {
            self.add_to_inflight_batches(&batches);
            self.send_write_requests(batches).await?;
        }

        Ok(())
    }

    fn add_to_inflight_batches(&self, batches: &HashMap<i32, Vec<ReadyWriteBatch>>) {
        let mut in_flight = self.in_flight_batches.lock();
        for batch_list in batches.values() {
            for batch in batch_list {
                in_flight
                    .entry(batch.table_bucket.clone())
                    .or_default()
                    .push(batch.write_batch.batch_id());
            }
        }
    }

    async fn send_write_requests(
        &self,
        collated: HashMap<i32, Vec<ReadyWriteBatch>>,
    ) -> Result<()> {
        for (leader_id, batches) in collated {
            self.send_write_request(leader_id, self.ack, batches)
                .await?;
        }
        Ok(())
    }

    async fn send_write_request(
        &self,
        destination: i32,
        acks: i16,
        batches: Vec<ReadyWriteBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut records_by_bucket = HashMap::new();
        let mut write_batch_by_table: HashMap<i64, Vec<TableBucket>> = HashMap::new();

        for batch in batches {
            let table_bucket = batch.table_bucket.clone();
            write_batch_by_table
                .entry(table_bucket.table_id())
                .or_default()
                .push(table_bucket.clone());
            records_by_bucket.insert(table_bucket, batch);
        }

        let cluster = self.metadata.get_cluster();

        let destination_node = match cluster.get_tablet_server(destination) {
            Some(node) => node,
            None => {
                self.handle_batches_with_error(
                    records_by_bucket.into_values().collect(),
                    FlussError::LeaderNotAvailableException,
                    format!("Destination node not found in metadata cache {destination}."),
                )
                .await?;
                return Ok(());
            }
        };
        let connection = match self.metadata.get_connection(destination_node).await {
            Ok(connection) => connection,
            Err(e) => {
                self.handle_batches_with_error(
                    records_by_bucket.into_values().collect(),
                    FlussError::NetworkException,
                    format!("Failed to connect destination node {destination}: {e}"),
                )
                .await?;
                return Ok(());
            }
        };

        for (table_id, table_buckets) in write_batch_by_table {
            let mut request_batches: Vec<ReadyWriteBatch> = table_buckets
                .iter()
                .filter_map(|bucket| records_by_bucket.remove(bucket))
                .collect();

            let write_request = match Self::build_write_request(
                table_id,
                acks,
                self.max_request_timeout_ms,
                &mut request_batches,
            ) {
                Ok(None) => continue, // Skip empty batches
                Ok(Some(req)) => req,
                Err(e) => {
                    self.handle_batches_with_local_error(
                        request_batches,
                        format!("Failed to build write request: {e}"),
                    )
                    .await?;
                    continue;
                }
            };

            // let's put in back into records_by_bucket
            // since response handle will use it.
            for request_batch in request_batches {
                records_by_bucket.insert(request_batch.table_bucket.clone(), request_batch);
            }

            self.send_and_handle_response(
                &connection,
                write_request,
                table_id,
                &table_buckets,
                &mut records_by_bucket,
            )
            .await?;
        }

        Ok(())
    }

    fn build_write_request(
        table_id: i64,
        acks: i16,
        timeout_ms: i32,
        request_batches: &mut [ReadyWriteBatch],
    ) -> Result<Option<WriteRequest>> {
        let first_batch = match request_batches.first() {
            None => return Ok(None),
            Some(b) => &b.write_batch,
        };

        let request = match first_batch {
            WriteBatch::ArrowLog(_) => {
                let req = ProduceLogRequest::new(table_id, acks, timeout_ms, request_batches)?;
                WriteRequest::ProduceLog(req)
            }
            WriteBatch::Kv(kv_write_batch) => {
                let target_columns = kv_write_batch.target_columns.clone();
                for batch in request_batches.iter() {
                    if let WriteBatch::Kv(kvb) = &batch.write_batch {
                        if target_columns != kvb.target_columns {
                            todo!("Target columns mismatch");
                        }
                    } else {
                        todo!("Non KvWriteBatch found")
                    }
                }
                let cols = target_columns
                    .map(|arc| arc.iter().map(|&c| c as i32).collect())
                    .unwrap_or_default();
                let req = PutKvRequest::new(table_id, acks, timeout_ms, cols, request_batches)?;
                WriteRequest::PutKv(req)
            }
        };

        Ok(Some(request))
    }

    async fn send_and_handle_response(
        &self,
        connection: &ServerConnection,
        write_request: WriteRequest,
        table_id: i64,
        table_buckets: &[TableBucket],
        records_by_bucket: &mut HashMap<TableBucket, ReadyWriteBatch>,
    ) -> Result<()> {
        macro_rules! send {
            ($request:expr) => {
                match connection.request($request).await {
                    Ok(response) => {
                        self.handle_write_response(
                            table_id,
                            table_buckets,
                            records_by_bucket,
                            response,
                        )
                        .await
                    }
                    Err(e) => {
                        self.handle_batches_with_error(
                            table_buckets
                                .iter()
                                .filter_map(|b| records_by_bucket.remove(b))
                                .collect(),
                            FlussError::NetworkException,
                            format!("Failed to send write request: {e}"),
                        )
                        .await
                    }
                }
            };
        }

        match write_request {
            WriteRequest::ProduceLog(req) => send!(req),
            WriteRequest::PutKv(req) => send!(req),
        }
    }

    async fn handle_write_response<R: WriteResponse>(
        &self,
        table_id: i64,
        request_buckets: &[TableBucket],
        records_by_bucket: &mut HashMap<TableBucket, ReadyWriteBatch>,
        response: R,
    ) -> Result<()> {
        let mut invalid_metadata_tables: HashSet<TablePath> = HashSet::new();
        let mut pending_buckets: HashSet<TableBucket> = request_buckets.iter().cloned().collect();

        for bucket_resp in response.buckets_resp() {
            let tb = TableBucket::new(table_id, bucket_resp.bucket_id());
            let Some(ready_batch) = records_by_bucket.remove(&tb) else {
                panic!("Missing ready batch for table bucket {tb}");
            };
            pending_buckets.remove(&tb);

            match bucket_resp.error_code() {
                Some(code) if code != FlussError::None.code() => {
                    let error = FlussError::for_code(code);
                    let message = bucket_resp
                        .error_message()
                        .cloned()
                        .unwrap_or_else(|| error.message().to_string());
                    if let Some(table_path) = self
                        .handle_write_batch_error(ready_batch, error, message)
                        .await?
                    {
                        invalid_metadata_tables.insert(table_path);
                    }
                }
                _ => self.complete_batch(ready_batch),
            }
        }

        for bucket in pending_buckets {
            if let Some(ready_batch) = records_by_bucket.remove(&bucket) {
                if let Some(table_path) = self
                    .handle_write_batch_error(
                        ready_batch,
                        FlussError::UnknownServerError,
                        format!("Missing response for table bucket {bucket}"),
                    )
                    .await?
                {
                    invalid_metadata_tables.insert(table_path);
                }
            }
        }

        self.update_metadata_if_needed(invalid_metadata_tables)
            .await;
        Ok(())
    }

    fn complete_batch(&self, ready_write_batch: ReadyWriteBatch) {
        self.finish_batch(ready_write_batch, Ok(()));
    }

    fn fail_batch(&self, ready_write_batch: ReadyWriteBatch, error: broadcast::Error) {
        self.finish_batch(ready_write_batch, Err(error));
    }

    fn finish_batch(&self, ready_write_batch: ReadyWriteBatch, result: broadcast::Result<()>) {
        if ready_write_batch.write_batch.complete(result) {
            self.remove_from_inflight_batches(&ready_write_batch);
            // remove from incomplete batches
            self.accumulator
                .remove_incomplete_batches(ready_write_batch.write_batch.batch_id())
        }
    }

    async fn handle_batches_with_error(
        &self,
        batches: Vec<ReadyWriteBatch>,
        error: FlussError,
        message: String,
    ) -> Result<()> {
        let mut invalid_metadata_tables: HashSet<TablePath> = HashSet::new();
        for batch in batches {
            if let Some(table_path) = self
                .handle_write_batch_error(batch, error, message.clone())
                .await?
            {
                invalid_metadata_tables.insert(table_path);
            }
        }
        self.update_metadata_if_needed(invalid_metadata_tables)
            .await;
        Ok(())
    }

    async fn handle_batches_with_local_error(
        &self,
        batches: Vec<ReadyWriteBatch>,
        message: String,
    ) -> Result<()> {
        for batch in batches {
            self.fail_batch(
                batch,
                broadcast::Error::Client {
                    message: message.clone(),
                },
            );
        }
        Ok(())
    }

    async fn handle_write_batch_error(
        &self,
        ready_write_batch: ReadyWriteBatch,
        error: FlussError,
        message: String,
    ) -> Result<Option<TablePath>> {
        let table_path = ready_write_batch.write_batch.table_path().clone();
        if self.can_retry(&ready_write_batch, error) {
            warn!(
                "Retrying write batch for {table_path} on bucket {} after error {error:?}: {message}",
                ready_write_batch.table_bucket.bucket_id()
            );
            self.re_enqueue_batch(ready_write_batch).await;
            return Ok(Self::is_invalid_metadata_error(error).then_some(table_path));
        }

        if error == FlussError::DuplicateSequenceException {
            warn!(
                "Duplicate sequence for {table_path} on bucket {}: {message}",
                ready_write_batch.table_bucket.bucket_id()
            );
            self.complete_batch(ready_write_batch);
            return Ok(None);
        }

        self.fail_batch(
            ready_write_batch,
            broadcast::Error::WriteFailed {
                code: error.code(),
                message,
            },
        );
        Ok(Self::is_invalid_metadata_error(error).then_some(table_path))
    }

    async fn re_enqueue_batch(&self, ready_write_batch: ReadyWriteBatch) {
        self.remove_from_inflight_batches(&ready_write_batch);
        self.accumulator.re_enqueue(ready_write_batch).await;
    }

    fn remove_from_inflight_batches(&self, ready_write_batch: &ReadyWriteBatch) {
        let batch_id = ready_write_batch.write_batch.batch_id();
        let mut in_flight_guard = self.in_flight_batches.lock();
        if let Some(in_flight) = in_flight_guard.get_mut(&ready_write_batch.table_bucket) {
            in_flight.retain(|id| *id != batch_id);
            if in_flight.is_empty() {
                in_flight_guard.remove(&ready_write_batch.table_bucket);
            }
        }
    }

    fn can_retry(&self, ready_write_batch: &ReadyWriteBatch, error: FlussError) -> bool {
        ready_write_batch.write_batch.attempts() < self.retries
            && !ready_write_batch.write_batch.is_done()
            && Self::is_retriable_error(error)
    }

    async fn update_metadata_if_needed(&self, table_paths: HashSet<TablePath>) {
        if table_paths.is_empty() {
            return;
        }
        let table_path_refs: HashSet<&TablePath> = table_paths.iter().collect();
        if let Err(e) = self.metadata.update_tables_metadata(&table_path_refs).await {
            warn!("Failed to update metadata after write error: {e:?}");
        }
    }

    fn is_invalid_metadata_error(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NotLeaderOrFollower
                | FlussError::UnknownTableOrBucketException
                | FlussError::LeaderNotAvailableException
                | FlussError::NetworkException
        )
    }

    fn is_retriable_error(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NetworkException
                | FlussError::NotLeaderOrFollower
                | FlussError::UnknownTableOrBucketException
                | FlussError::LeaderNotAvailableException
                | FlussError::LogStorageException
                | FlussError::KvStorageException
                | FlussError::StorageException
                | FlussError::RequestTimeOut
                | FlussError::NotEnoughReplicasAfterAppendException
                | FlussError::NotEnoughReplicasException
                | FlussError::CorruptMessage
                | FlussError::CorruptRecordException
        )
    }

    pub async fn close(&mut self) {
        self.running = false;
    }
}

enum WriteRequest {
    ProduceLog(ProduceLogRequest),
    PutKv(PutKvRequest),
}

trait BucketResponse {
    fn bucket_id(&self) -> i32;
    fn error_code(&self) -> Option<i32>;
    fn error_message(&self) -> Option<&String>;
}

impl BucketResponse for PbProduceLogRespForBucket {
    fn bucket_id(&self) -> i32 {
        self.bucket_id
    }
    fn error_code(&self) -> Option<i32> {
        self.error_code
    }
    fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

impl BucketResponse for PbPutKvRespForBucket {
    fn bucket_id(&self) -> i32 {
        self.bucket_id
    }
    fn error_code(&self) -> Option<i32> {
        self.error_code
    }
    fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

trait WriteResponse {
    type BucketResp: BucketResponse;
    fn buckets_resp(&self) -> &[Self::BucketResp];
}

impl WriteResponse for ProduceLogResponse {
    type BucketResp = PbProduceLogRespForBucket;
    fn buckets_resp(&self) -> &[Self::BucketResp] {
        &self.buckets_resp
    }
}

impl WriteResponse for PutKvResponse {
    type BucketResp = PbPutKvRespForBucket;
    fn buckets_resp(&self) -> &[Self::BucketResp] {
        &self.buckets_resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::cluster::Cluster;
    use crate::config::Config;
    use crate::metadata::TablePath;
    use crate::proto::{PbProduceLogRespForBucket, ProduceLogResponse};
    use crate::row::{Datum, GenericRow};
    use crate::rpc::FlussError;
    use crate::test_utils::build_cluster_arc;
    use std::collections::{HashMap, HashSet};

    async fn build_ready_batch(
        accumulator: &RecordAccumulator,
        cluster: Arc<Cluster>,
        table_path: Arc<TablePath>,
    ) -> Result<(ReadyWriteBatch, crate::client::ResultHandle)> {
        let record = WriteRecord::for_append(
            table_path,
            1,
            GenericRow {
                values: vec![Datum::Int32(1)],
            },
        );
        let result = accumulator.append(&record, 0, &cluster, false).await?;
        let result_handle = result.result_handle.expect("result handle");
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024).await?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        Ok((batch, result_handle))
    }

    #[tokio::test]
    async fn handle_write_batch_error_retries() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(metadata, accumulator.clone(), 1024 * 1024, 1000, 1, 1);

        let (batch, _handle) =
            build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path.clone()).await?;
        let mut inflight = HashMap::new();
        inflight.insert(1, vec![batch]);
        sender.add_to_inflight_batches(&inflight);
        let batch = inflight.remove(&1).unwrap().pop().unwrap();

        sender
            .handle_write_batch_error(batch, FlussError::RequestTimeOut, "timeout".to_string())
            .await?;

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024).await?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn handle_write_batch_error_fails() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(metadata, accumulator.clone(), 1024 * 1024, 1000, 1, 0);

        let (batch, handle) =
            build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path).await?;
        sender
            .handle_write_batch_error(
                batch,
                FlussError::InvalidTableException,
                "invalid".to_string(),
            )
            .await?;

        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::InvalidTableException.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn handle_produce_response_duplicate_sequence_completes() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(metadata, accumulator.clone(), 1024 * 1024, 1000, 1, 0);

        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster, table_path).await?;
        let request_buckets = vec![batch.table_bucket.clone()];
        let mut records_by_bucket = HashMap::new();
        records_by_bucket.insert(batch.table_bucket.clone(), batch);

        let response = ProduceLogResponse {
            buckets_resp: vec![PbProduceLogRespForBucket {
                bucket_id: 0,
                error_code: Some(FlussError::DuplicateSequenceException.code()),
                error_message: Some("dup".to_string()),
                ..Default::default()
            }],
        };

        sender
            .handle_write_response(1, &request_buckets, &mut records_by_bucket, response)
            .await?;

        let batch_result = handle.wait().await?;
        assert!(matches!(batch_result, Ok(())));
        Ok(())
    }
}
