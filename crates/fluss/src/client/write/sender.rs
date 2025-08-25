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

use crate::client::metadata::Metadata;
use crate::client::{ReadyWriteBatch, RecordAccumulator};
use crate::error::Error::WriteError;
use crate::error::Result;
use crate::metadata::TableBucket;
use crate::proto::ProduceLogResponse;
use crate::rpc::ProduceLogRequest;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[allow(dead_code)]
pub struct Sender {
    running: bool,
    metadata: Arc<Metadata>,
    accumulator: Arc<RecordAccumulator>,
    in_flight_batches: Mutex<HashMap<TableBucket, Vec<Arc<ReadyWriteBatch>>>>,
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
            self.send_write_requests(&batches).await?;
        }

        Ok(())
    }

    fn add_to_inflight_batches(&self, batches: &HashMap<i32, Vec<Arc<ReadyWriteBatch>>>) {
        let mut in_flight = self.in_flight_batches.lock();
        for batch_list in batches.values() {
            for batch in batch_list {
                in_flight
                    .entry(batch.table_bucket.clone())
                    .or_default()
                    .push(batch.clone());
            }
        }
    }

    async fn send_write_requests(
        &self,
        collated: &HashMap<i32, Vec<Arc<ReadyWriteBatch>>>,
    ) -> Result<()> {
        for (leader_id, batches) in collated {
            println!("send request batch");
            self.send_write_request(*leader_id, self.ack, batches)
                .await?;
        }
        Ok(())
    }

    async fn send_write_request(
        &self,
        destination: i32,
        acks: i16,
        batches: &Vec<Arc<ReadyWriteBatch>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut records_by_bucket = HashMap::new();
        let mut write_batch_by_table = HashMap::new();

        for batch in batches {
            records_by_bucket.insert(batch.table_bucket.clone(), batch.clone());
            write_batch_by_table
                .entry(batch.table_bucket.table_id())
                .or_insert_with(Vec::new)
                .push(batch);
        }

        let cluster = self.metadata.get_cluster();

        let destination_node = cluster
            .get_tablet_server(destination)
            .ok_or(WriteError(String::from("destination node not found")))?;
        let connection = self.metadata.get_connection(destination_node).await?;

        for (table_id, write_batches) in write_batch_by_table {
            let request =
                ProduceLogRequest::new(table_id, acks, self.max_request_timeout_ms, write_batches)?;
            let response = connection.request(request).await?;
            self.handle_produce_response(table_id, &records_by_bucket, response)?
        }

        Ok(())
    }

    fn handle_produce_response(
        &self,
        table_id: i64,
        records_by_bucket: &HashMap<TableBucket, Arc<ReadyWriteBatch>>,
        response: ProduceLogResponse,
    ) -> Result<()> {
        for produce_log_response_for_bucket in response.buckets_resp.iter() {
            let tb = TableBucket::new(table_id, produce_log_response_for_bucket.bucket_id);

            let ready_batch = records_by_bucket.get(&tb).unwrap();
            if let Some(error_code) = produce_log_response_for_bucket.error_code {
                todo!("handle_produce_response error: {}", error_code)
            } else {
                self.complete_batch(ready_batch)
            }
        }
        Ok(())
    }

    fn complete_batch(&self, ready_write_batch: &Arc<ReadyWriteBatch>) {
        if ready_write_batch.write_batch.complete(Ok(())) {
            // remove from in flight batches
            let mut in_flight_guard = self.in_flight_batches.lock();
            if let Some(in_flight) = in_flight_guard.get_mut(&ready_write_batch.table_bucket) {
                in_flight.retain(|b| !Arc::ptr_eq(b, ready_write_batch));
                if in_flight.is_empty() {
                    in_flight_guard.remove(&ready_write_batch.table_bucket);
                }
            }
            // remove from incomplete batches
            self.accumulator
                .remove_incomplete_batches(ready_write_batch.write_batch.batch_id())
        }
    }

    pub async fn close(&mut self) {
        self.running = false;
    }
}
