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

mod accumulator;
mod batch;

use crate::client::broadcast::{self as client_broadcast, BatchWriteResult, BroadcastOnceReceiver};
use crate::error::{Error, Result};
use crate::metadata::TablePath;
use crate::row::{BinaryRow, GenericRow};
pub use accumulator::*;
use arrow::array::RecordBatch;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) mod broadcast;
mod bucket_assigner;

mod sender;
mod write_format;
mod writer_client;

use crate::error::Error::IllegalArgument;
use crate::record::kv::{KvRecord, RECORD_BATCH_HEADER_SIZE};
use crate::row::compacted::CompactedRow;
pub use write_format::WriteFormat;
pub use writer_client::WriterClient;

pub struct WriteRecord<'a> {
    pub row: Record<'a>,
    pub table_path: Arc<TablePath>,
}

pub enum Record<'a> {
    KvRow(KvRow<CompactedRow<'a>>),
    Row(GenericRow<'a>),
    RecordBatch(Arc<RecordBatch>),
}

impl<'a> WriteRecord<'a> {
    pub fn new(table_path: Arc<TablePath>, row: GenericRow<'a>) -> Self {
        Self {
            row: Record::Row(row),
            table_path,
        }
    }

    pub fn new_record_batch(table_path: Arc<TablePath>, row: RecordBatch) -> Self {
        Self {
            row: Record::RecordBatch(Arc::new(row)),
            table_path,
        }
    }

    pub fn for_upsert(
        table_path: Arc<TablePath>,
        row: CompactedRow<'a>,
        key: Bytes,
        bucket_key: Option<Bytes>,
        write_format: &WriteFormat,
        target_columns: Arc<[usize]>,
    ) -> Result<Self> {
        if !write_format.is_kv() {
            return Err(IllegalArgument {
                message: format!("writeFormat must be a KV format, got {}", write_format),
            });
        }

        let estimated_size_in_bytes =
            KvRecord::size_of(key.as_ref(), Some(row.as_bytes())) + RECORD_BATCH_HEADER_SIZE;

        Ok(Self {
            row: Record::KvRow(KvRow::new(
                row,
                key,
                bucket_key,
                target_columns,
                estimated_size_in_bytes,
            )),
            table_path,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct KvRow<B: BinaryRow> {
    row: B,
    key: Bytes,
    bucket_key: Option<Bytes>,
    target_columns: Arc<[usize]>,
    estimated_size_in_bytes: usize,
}

impl<B: BinaryRow> KvRow<B> {
    pub fn new(
        row: B,
        key: Bytes,
        bucket_key: Option<Bytes>,
        target_columns: Arc<[usize]>,
        estimated_size_in_bytes: usize,
    ) -> Self {
        Self {
            row,
            key,
            bucket_key,
            target_columns,
            estimated_size_in_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResultHandle {
    receiver: BroadcastOnceReceiver<BatchWriteResult>,
}

impl ResultHandle {
    pub fn new(receiver: BroadcastOnceReceiver<BatchWriteResult>) -> Self {
        ResultHandle { receiver }
    }

    pub async fn wait(&self) -> Result<BatchWriteResult> {
        self.receiver
            .receive()
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Fail to wait write result {e:?}"),
                source: None,
            })
    }

    pub fn result(&self, batch_result: BatchWriteResult) -> Result<()> {
        batch_result.map_err(|e| match e {
            client_broadcast::Error::WriteFailed { code, message } => Error::FlussAPIError {
                api_error: crate::rpc::ApiError { code, message },
            },
            client_broadcast::Error::Client { message } => Error::UnexpectedError {
                message,
                source: None,
            },
            client_broadcast::Error::Dropped => Error::UnexpectedError {
                message: "Fail to get write result because broadcast was dropped.".to_string(),
                source: None,
            },
        })
    }
}
