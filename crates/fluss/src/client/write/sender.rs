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
use crate::client::{ReadyWriteBatch, RecordAccumulator};
use crate::error::{FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::ProduceLogResponse;
use crate::rpc::message::ProduceLogRequest;
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
            self.send_write_request(leader_id, self.ack, batches).await?;
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
        let mut records_by_bucket: HashMap<TableBucket, ReadyWriteBatch> = HashMap::new();
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

        let destination_node =
            match cluster.get_tablet_server(destination) {
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
            let request_batches: Vec<&ReadyWriteBatch> = table_buckets
                .iter()
                .filter_map(|bucket| records_by_bucket.get(bucket))
                .collect();
            if request_batches.is_empty() {
                continue;
            }
            let request = match ProduceLogRequest::new(
                table_id,
                acks,
                self.max_request_timeout_ms,
                request_batches.as_slice(),
            ) {
                Ok(request) => request,
                Err(e) => {
                    self.handle_batches_with_error(
                        table_buckets
                            .iter()
                            .filter_map(|bucket| records_by_bucket.remove(bucket))
                            .collect(),
                        FlussError::UnknownServerError,
                        format!("Failed to build produce request: {e}"),
                    )
                    .await?;
                    continue;
                }
            };

            let response = match connection.request(request).await {
                Ok(response) => response,
                Err(e) => {
                    self.handle_batches_with_error(
                        table_buckets
                            .iter()
                            .filter_map(|bucket| records_by_bucket.remove(bucket))
                            .collect(),
                        FlussError::NetworkException,
                        format!("Failed to send produce request: {e}"),
                    )
                    .await?;
                    continue;
                }
            };

            self.handle_produce_response(
                table_id,
                &table_buckets,
                &mut records_by_bucket,
                response,
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_produce_response(
        &self,
        table_id: i64,
        request_buckets: &[TableBucket],
        records_by_bucket: &mut HashMap<TableBucket, ReadyWriteBatch>,
        response: ProduceLogResponse,
    ) -> Result<()> {
        let mut invalid_metadata_tables: HashSet<TablePath> = HashSet::new();
        let mut pending_buckets: HashSet<TableBucket> = request_buckets.iter().cloned().collect();
        for produce_log_response_for_bucket in response.buckets_resp.iter() {
            let tb = TableBucket::new(table_id, produce_log_response_for_bucket.bucket_id);

            let Some(ready_batch) = records_by_bucket.remove(&tb) else {
                warn!("Missing ready batch for table bucket {tb}");
                continue;
            };
            pending_buckets.remove(&tb);

            if let Some(error_code) = produce_log_response_for_bucket.error_code {
                if error_code == FlussError::None.code() {
                    self.complete_batch(ready_batch);
                    continue;
                }

                let error = FlussError::for_code(error_code);
                let message = produce_log_response_for_bucket
                    .error_message
                    .clone()
                    .unwrap_or_else(|| error.message().to_string());
                if let Some(table_path) =
                    self.handle_write_batch_error(ready_batch, error, message).await?
                {
                    invalid_metadata_tables.insert(table_path);
                }
            } else {
                self.complete_batch(ready_batch)
            }
        }
        if !pending_buckets.is_empty() {
            for bucket in pending_buckets {
                if let Some(ready_batch) = records_by_bucket.remove(&bucket) {
                    let message = format!(
                        "Missing response for table bucket {bucket} in produce response."
                    );
                    let error = FlussError::UnknownServerError;
                    if let Some(table_path) =
                        self.handle_write_batch_error(ready_batch, error, message).await?
                    {
                        invalid_metadata_tables.insert(table_path);
                    }
                }
            }
        }
        self.update_metadata_if_needed(invalid_metadata_tables).await;
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
            if let Some(table_path) =
                self.handle_write_batch_error(batch, error, message.clone()).await?
            {
                invalid_metadata_tables.insert(table_path);
            }
        }
        self.update_metadata_if_needed(invalid_metadata_tables).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::config::Config;
    use crate::metadata::{DataField, DataTypes, Schema, TableDescriptor, TableInfo, TablePath};
    use crate::row::{Datum, GenericRow};
    use crate::rpc::FlussError;
    use crate::proto::{PbProduceLogRespForBucket, ProduceLogResponse};
    use std::collections::HashSet;

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

    fn build_cluster(table_path: &TablePath, table_id: i64) -> Arc<Cluster> {
        let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let table_bucket = TableBucket::new(table_id, 0);
        let bucket_location =
            BucketLocation::new(table_bucket.clone(), Some(server.clone()), table_path.clone());

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

        Arc::new(Cluster::new(
            None,
            servers,
            locations_by_path,
            locations_by_bucket,
            table_id_by_path,
            table_info_by_path,
        ))
    }

    async fn build_ready_batch(
        accumulator: &RecordAccumulator,
        cluster: Arc<Cluster>,
        table_path: Arc<TablePath>,
    ) -> Result<(ReadyWriteBatch, crate::client::ResultHandle)> {
        let record = WriteRecord::new(
            table_path,
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
        let cluster = build_cluster(table_path.as_ref(), 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            1,
        );

        let (batch, _handle) =
            build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path.clone()).await?;
        let mut inflight = HashMap::new();
        inflight.insert(1, vec![batch]);
        sender.add_to_inflight_batches(&inflight);
        let batch = inflight.remove(&1).unwrap().pop().unwrap();

        sender
            .handle_write_batch_error(
                batch,
                FlussError::RequestTimeOut,
                "timeout".to_string(),
            )
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
        let cluster = build_cluster(table_path.as_ref(), 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            0,
        );

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
    async fn handle_produce_response_missing_bucket_fails() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster(table_path.as_ref(), 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            0,
        );

        let (batch, handle) =
            build_ready_batch(accumulator.as_ref(), cluster, table_path).await?;
        let request_buckets = vec![batch.table_bucket.clone()];
        let mut records_by_bucket = HashMap::new();
        records_by_bucket.insert(batch.table_bucket.clone(), batch);

        let response = ProduceLogResponse {
            buckets_resp: vec![PbProduceLogRespForBucket {
                bucket_id: 1,
                error_code: None,
                ..Default::default()
            }],
        };

        sender
            .handle_produce_response(1, &request_buckets, &mut records_by_bucket, response)
            .await?;

        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::UnknownServerError.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn handle_produce_response_duplicate_sequence_completes() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster(table_path.as_ref(), 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let accumulator = Arc::new(RecordAccumulator::new(Config::default()));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            0,
        );

        let (batch, handle) =
            build_ready_batch(accumulator.as_ref(), cluster, table_path).await?;
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
            .handle_produce_response(1, &request_buckets, &mut records_by_bucket, response)
            .await?;

        let batch_result = handle.wait().await?;
        assert!(matches!(batch_result, Ok(())));
        Ok(())
    }
}
