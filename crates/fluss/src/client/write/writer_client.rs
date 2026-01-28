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
use crate::client::write::bucket_assigner::{BucketAssigner, StickyBucketAssigner};
use crate::client::write::sender::Sender;
use crate::client::{RecordAccumulator, ResultHandle, WriteRecord};
use crate::config::Config;
use crate::metadata::PhysicalTablePath;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

#[allow(dead_code)]
pub struct WriterClient {
    config: Config,
    max_request_size: i32,
    accumulate: Arc<RecordAccumulator>,
    shutdown_tx: mpsc::Sender<()>,
    sender_join_handle: JoinHandle<()>,
    metadata: Arc<Metadata>,
    bucket_assigners: DashMap<Arc<PhysicalTablePath>, Arc<Box<dyn BucketAssigner>>>,
}

impl WriterClient {
    pub fn new(config: Config, metadata: Arc<Metadata>) -> Result<Self> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        let accumulator = Arc::new(RecordAccumulator::new(config.clone()));

        let mut sender = Sender::new(
            metadata.clone(),
            accumulator.clone(),
            config.request_max_size,
            30_000,
            Self::get_ack(&config)?,
            config.writer_retries,
        );

        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = sender.run() => {
                    // do-nothing
                },
                _ = shutdown_rx.recv() => {
                    sender.close().await
                }
            }
        });

        Ok(Self {
            max_request_size: config.request_max_size,
            config,
            shutdown_tx,
            sender_join_handle: join_handle,
            accumulate: accumulator,
            metadata,
            bucket_assigners: Default::default(),
        })
    }

    fn get_ack(config: &Config) -> Result<i16> {
        let acks = config.writer_acks.as_str();
        if acks.eq_ignore_ascii_case("all") {
            Ok(-1)
        } else {
            acks.parse::<i16>().map_err(|e| Error::IllegalArgument {
                message: format!("invalid writer ack '{acks}': {e}"),
            })
        }
    }

    pub async fn send(&self, record: &WriteRecord<'_>) -> Result<ResultHandle> {
        let physical_table_path = &record.physical_table_path;
        let cluster = self.metadata.get_cluster();
        let bucket_key = record.bucket_key.as_ref();

        let (bucket_assigner, bucket_id) = self.assign_bucket(bucket_key, physical_table_path)?;

        let mut result = self
            .accumulate
            .append(record, bucket_id, &cluster, true)
            .await?;

        if result.abort_record_for_new_batch {
            let prev_bucket_id = bucket_id;
            bucket_assigner.on_new_batch(&cluster, prev_bucket_id);
            let bucket_id = bucket_assigner.assign_bucket(bucket_key, &cluster)?;
            result = self
                .accumulate
                .append(record, bucket_id, &cluster, false)
                .await?;
        }

        if result.batch_is_full || result.new_batch_created {
            // todo: wakeup
        }

        Ok(result.result_handle.expect("result_handle should exist"))
    }
    fn assign_bucket(
        &self,
        bucket_key: Option<&Bytes>,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Result<(Arc<Box<dyn BucketAssigner>>, i32)> {
        let cluster = self.metadata.get_cluster();
        let bucket_assigner = {
            if let Some(assigner) = self.bucket_assigners.get(table_path) {
                assigner.clone()
            } else {
                let assigner = Arc::new(Self::create_bucket_assigner(Arc::clone(table_path)));
                self.bucket_assigners
                    .insert(Arc::clone(table_path), assigner.clone());
                assigner
            }
        };
        let bucket_id = bucket_assigner.assign_bucket(bucket_key, &cluster)?;
        Ok((bucket_assigner, bucket_id))
    }

    pub async fn close(self) -> Result<()> {
        self.shutdown_tx
            .send(())
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to close write client: {e:?}"),
                source: None,
            })?;

        self.sender_join_handle
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to close write client: {e:?}"),
                source: None,
            })?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.accumulate.begin_flush();
        self.accumulate.await_flush_completion().await?;
        Ok(())
    }

    pub fn create_bucket_assigner(table_path: Arc<PhysicalTablePath>) -> Box<dyn BucketAssigner> {
        // always sticky
        Box::new(StickyBucketAssigner::new(table_path))
    }
}
