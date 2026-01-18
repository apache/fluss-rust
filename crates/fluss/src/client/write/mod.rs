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
use crate::error::Error;
use crate::metadata::TablePath;
use crate::row::{CompactedRow, GenericRow};
pub use accumulator::*;
use arrow::array::RecordBatch;
use std::sync::Arc;

pub(crate) mod broadcast;
mod bucket_assigner;

mod sender;
mod write_format;
mod writer_client;

pub use write_format::WriteFormat;
pub use writer_client::WriterClient;

#[allow(dead_code)]
pub struct WriteRecord<'a> {
    record: Record<'a>,
    table_path: Arc<TablePath>,
    bucket_key: Option<&'a [u8]>,
    schema_id: i32,
    write_format: WriteFormat,
}

impl<'a> WriteRecord<'a> {
    pub fn record(&self) -> &Record<'a> {
        &self.record
    }
}

pub enum Record<'a> {
    Log(LogWriteRecord<'a>),
    Kv(KvWriteRecord<'a>),
}

pub enum LogWriteRecord<'a> {
    Generic(GenericRow<'a>),
    RecordBatch(Arc<RecordBatch>),
}

pub struct KvWriteRecord<'a> {
    // only valid for primary key table
    key: &'a [u8],
    target_columns: Option<&'a [usize]>,
    compacted_row: Option<CompactedRow<'a>>,
}

impl<'a> KvWriteRecord<'a> {
    fn new(
        key: &'a [u8],
        target_columns: Option<&'a [usize]>,
        compacted_row: Option<CompactedRow<'a>>,
    ) -> Self {
        KvWriteRecord {
            key,
            target_columns,
            compacted_row,
        }
    }
}

impl<'a> WriteRecord<'a> {
    pub fn for_append(table_path: Arc<TablePath>, schema_id: i32, row: GenericRow<'a>) -> Self {
        Self {
            record: Record::Log(LogWriteRecord::Generic(row)),
            table_path,
            bucket_key: None,
            schema_id,
            write_format: WriteFormat::ArrowLog,
        }
    }

    pub fn for_append_record_batch(
        table_path: Arc<TablePath>,
        schema_id: i32,
        row: RecordBatch,
    ) -> Self {
        Self {
            record: Record::Log(LogWriteRecord::RecordBatch(Arc::new(row))),
            table_path,
            bucket_key: None,
            schema_id,
            write_format: WriteFormat::ArrowLog,
        }
    }

    pub fn for_upsert(
        table_path: Arc<TablePath>,
        schema_id: i32,
        bucket_key: &'a [u8],
        key: &'a [u8],
        target_columns: &'a [usize],
        row: CompactedRow<'a>,
    ) -> Self {
        Self {
            record: Record::Kv(KvWriteRecord::new(key, Some(target_columns), Some(row))),
            table_path,
            bucket_key: Some(bucket_key),
            schema_id,
            write_format: WriteFormat::CompactedKv,
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

    pub async fn wait(&self) -> Result<BatchWriteResult, Error> {
        self.receiver
            .receive()
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Fail to wait write result {e:?}"),
                source: None,
            })
    }

    pub fn result(&self, batch_result: BatchWriteResult) -> Result<(), Error> {
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
