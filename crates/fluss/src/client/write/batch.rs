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

use crate::BucketId;
use crate::client::broadcast::{BatchWriteResult, BroadcastOnce};
use crate::client::{Record, ResultHandle, WriteRecord};
use crate::compression::ArrowCompressionInfo;
use crate::error::{Error, Result};
use crate::metadata::{KvFormat, RowType, TablePath};
use crate::record::MemoryLogRecordsArrowBuilder;
use crate::record::kv::KvRecordBatchBuilder;
use bytes::Bytes;
use std::cmp::max;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

#[allow(dead_code)]
pub struct InnerWriteBatch {
    batch_id: i64,
    table_path: TablePath,
    create_ms: i64,
    bucket_id: BucketId,
    results: BroadcastOnce<BatchWriteResult>,
    completed: AtomicBool,
    attempts: AtomicI32,
    drained_ms: i64,
}

impl InnerWriteBatch {
    fn new(batch_id: i64, table_path: TablePath, create_ms: i64, bucket_id: BucketId) -> Self {
        InnerWriteBatch {
            batch_id,
            table_path,
            create_ms,
            bucket_id,
            results: Default::default(),
            completed: AtomicBool::new(false),
            attempts: AtomicI32::new(0),
            drained_ms: -1,
        }
    }

    fn waited_time_ms(&self, now: i64) -> i64 {
        max(0i64, now - self.create_ms)
    }

    fn complete(&self, write_result: BatchWriteResult) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.results.broadcast(write_result);
        true
    }

    fn drained(&mut self, now_ms: i64) {
        self.drained_ms = max(self.drained_ms, now_ms);
    }

    fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    fn attempts(&self) -> i32 {
        self.attempts.load(Ordering::Acquire)
    }

    fn re_enqueued(&self) {
        self.attempts.fetch_add(1, Ordering::AcqRel);
    }

    fn is_done(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

pub enum WriteBatch {
    ArrowLog(ArrowLogWriteBatch),
    Kv(KvWriteBatch),
}

impl WriteBatch {
    pub fn inner_batch(&self) -> &InnerWriteBatch {
        match self {
            WriteBatch::ArrowLog(batch) => &batch.write_batch,
            WriteBatch::Kv(batch) => &batch.write_batch,
        }
    }

    pub fn inner_batch_mut(&mut self) -> &mut InnerWriteBatch {
        match self {
            WriteBatch::ArrowLog(batch) => &mut batch.write_batch,
            WriteBatch::Kv(batch) => &mut batch.write_batch,
        }
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.try_append(write_record),
            WriteBatch::Kv(batch) => batch.try_append(write_record),
        }
    }

    pub fn waited_time_ms(&self, now: i64) -> i64 {
        self.inner_batch().waited_time_ms(now)
    }

    pub fn close(&mut self) -> Result<()> {
        match self {
            WriteBatch::ArrowLog(batch) => {
                batch.close();
                Ok(())
            }
            WriteBatch::Kv(batch) => batch.close(),
        }
    }

    pub fn estimated_size_in_bytes(&self) -> i64 {
        0
        // todo: calculate estimated_size_in_bytes
    }

    pub fn is_closed(&self) -> bool {
        match self {
            WriteBatch::ArrowLog(batch) => batch.is_closed(),
            WriteBatch::Kv(batch) => batch.is_closed(),
        }
    }

    pub fn drained(&mut self, now_ms: i64) {
        self.inner_batch_mut().drained(now_ms);
    }

    pub fn build(&mut self) -> Result<Bytes> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.build(),
            WriteBatch::Kv(batch) => batch.build(),
        }
    }

    pub fn complete(&self, write_result: BatchWriteResult) -> bool {
        self.inner_batch().complete(write_result)
    }

    pub fn batch_id(&self) -> i64 {
        self.inner_batch().batch_id
    }

    pub fn table_path(&self) -> &TablePath {
        self.inner_batch().table_path()
    }

    pub fn attempts(&self) -> i32 {
        self.inner_batch().attempts()
    }

    pub fn re_enqueued(&self) {
        self.inner_batch().re_enqueued();
    }

    pub fn is_done(&self) -> bool {
        self.inner_batch().is_done()
    }
}

pub struct ArrowLogWriteBatch {
    pub write_batch: InnerWriteBatch,
    pub arrow_builder: MemoryLogRecordsArrowBuilder,
    built_records: Option<Bytes>,
}

impl ArrowLogWriteBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: i64,
        table_path: TablePath,
        schema_id: i32,
        arrow_compression_info: ArrowCompressionInfo,
        row_type: &RowType,
        bucket_id: BucketId,
        create_ms: i64,
        to_append_record_batch: bool,
    ) -> Self {
        let base = InnerWriteBatch::new(batch_id, table_path, create_ms, bucket_id);
        Self {
            write_batch: base,
            arrow_builder: MemoryLogRecordsArrowBuilder::new(
                schema_id,
                row_type,
                to_append_record_batch,
                arrow_compression_info,
            ),
            built_records: None,
        }
    }

    pub fn batch_id(&self) -> i64 {
        self.write_batch.batch_id
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        if self.arrow_builder.is_closed() || self.arrow_builder.is_full() {
            Ok(None)
        } else {
            // append successfully
            if self.arrow_builder.append(write_record)? {
                Ok(Some(ResultHandle::new(self.write_batch.results.receiver())))
            } else {
                // append fail
                Ok(None)
            }
        }
    }

    pub fn build(&mut self) -> Result<Bytes> {
        if let Some(bytes) = &self.built_records {
            return Ok(bytes.clone());
        }
        let bytes = Bytes::from(self.arrow_builder.build()?);
        self.built_records = Some(bytes.clone());
        Ok(bytes)
    }

    pub fn is_closed(&self) -> bool {
        self.arrow_builder.is_closed()
    }

    pub fn close(&mut self) {
        self.arrow_builder.close()
    }
}

pub struct KvWriteBatch {
    write_batch: InnerWriteBatch,
    kv_batch_builder: KvRecordBatchBuilder,
    target_columns: Option<Vec<usize>>,
    schema_id: i32,
}

impl KvWriteBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: i64,
        table_path: TablePath,
        schema_id: i32,
        write_limit: usize,
        kv_format: KvFormat,
        bucket_id: BucketId,
        target_columns: Option<Vec<usize>>,
        create_ms: i64,
    ) -> Self {
        let base = InnerWriteBatch::new(batch_id, table_path, create_ms, bucket_id);
        Self {
            write_batch: base,
            kv_batch_builder: KvRecordBatchBuilder::new(schema_id, write_limit, kv_format),
            target_columns,
            schema_id,
        }
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        let kv_write_record = match &write_record.record {
            Record::Kv(record) => record,
            _ => {
                return Err(Error::UnsupportedOperation {
                    message: "Only KvRecord to append to KvWriteBatch ".to_string(),
                });
            }
        };

        let key = kv_write_record.key.as_ref();

        if self.schema_id != write_record.schema_id {
            return Err(Error::UnexpectedError {
                message: format!(
                    "schema id {} of the write record to append is not the same as the current schema id {} in the batch.",
                    write_record.schema_id, self.schema_id
                ),
                source: None,
            });
        };

        if self.target_columns.as_ref() != kv_write_record.target_columns.as_deref() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "target columns {:?} of the write record to append are not the same as the current target columns {:?} in the batch.",
                    kv_write_record.target_columns,
                    self.target_columns.as_deref()
                ),
                source: None,
            });
        }

        let row = kv_write_record.compacted_row.as_ref();

        if self.is_closed() || !self.kv_batch_builder.has_room_for_row(key, row) {
            Ok(None)
        } else {
            // append successfully
            self.kv_batch_builder
                .append_row(key, row)
                .map_err(|e| Error::UnexpectedError {
                    message: "Failed to append row to KvWriteBatch".to_string(),
                    source: Some(Box::new(e)),
                })?;
            Ok(Some(ResultHandle::new(self.write_batch.results.receiver())))
        }
    }

    pub fn build(&mut self) -> Result<Bytes> {
        self.kv_batch_builder.build()
    }

    pub fn is_closed(&self) -> bool {
        self.kv_batch_builder.is_closed()
    }

    pub fn close(&mut self) -> Result<()> {
        self.kv_batch_builder.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TablePath;

    #[test]
    fn complete_only_once() {
        let batch =
            InnerWriteBatch::new(1, TablePath::new("db".to_string(), "tbl".to_string()), 0, 0);
        assert!(batch.complete(Ok(())));
        assert!(!batch.complete(Err(crate::client::broadcast::Error::Dropped)));
    }

    #[test]
    fn attempts_increment_on_reenqueue() {
        let batch =
            InnerWriteBatch::new(1, TablePath::new("db".to_string(), "tbl".to_string()), 0, 0);
        assert_eq!(batch.attempts(), 0);
        batch.re_enqueued();
        assert_eq!(batch.attempts(), 1);
    }
}
