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
use crate::client::{ResultHandle, WriteRecord};
use crate::metadata::{DataType, TablePath};
use std::cmp::max;

use crate::error::Result;
use crate::record::MemoryLogRecordsArrowBuilder;

#[allow(dead_code)]
pub struct InnerWriteBatch {
    batch_id: i64,
    table_path: TablePath,
    create_ms: i64,
    bucket_id: BucketId,
    results: BroadcastOnce<BatchWriteResult>,
    completed: bool,
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
            completed: Default::default(),
            drained_ms: -1,
        }
    }

    fn waited_time_ms(&self, now: i64) -> i64 {
        max(0i64, now - self.create_ms)
    }

    fn complete(&self, write_result: BatchWriteResult) -> bool {
        if !self.completed {
            self.results.broadcast(write_result);
        }
        true
    }

    fn drained(&mut self, now_ms: i64) {
        self.drained_ms = max(self.drained_ms, now_ms);
    }
}

pub enum WriteBatch {
    ArrowLog(ArrowLogWriteBatch),
}

impl WriteBatch {
    pub fn inner_batch(&self) -> &InnerWriteBatch {
        match self {
            WriteBatch::ArrowLog(batch) => &batch.write_batch,
        }
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.try_append(write_record),
        }
    }

    pub fn waited_time_ms(&self, now: i64) -> i64 {
        self.inner_batch().waited_time_ms(now)
    }

    pub fn close(&mut self) {
        match self {
            WriteBatch::ArrowLog(batch) => {
                batch.close();
            }
        }
    }

    pub fn estimated_size_in_bytes(&self) -> i64 {
        0
        // todo: calculate estimated_size_in_bytes
    }

    pub fn is_closed(&self) -> bool {
        match self {
            WriteBatch::ArrowLog(batch) => batch.is_closed(),
        }
    }

    pub fn drained(&mut self, now_ms: i64) {
        match self {
            WriteBatch::ArrowLog(batch) => {
                batch.write_batch.drained(now_ms);
            }
        }
    }

    pub fn build(&self) -> Result<Vec<u8>> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.build(),
        }
    }

    pub fn complete(&self, write_result: BatchWriteResult) -> bool {
        self.inner_batch().complete(write_result)
    }

    pub fn batch_id(&self) -> i64 {
        self.inner_batch().batch_id
    }
}

pub struct ArrowLogWriteBatch {
    pub write_batch: InnerWriteBatch,
    pub arrow_builder: MemoryLogRecordsArrowBuilder,
}

impl ArrowLogWriteBatch {
    pub fn new(
        batch_id: i64,
        table_path: TablePath,
        schema_id: i32,
        row_type: &DataType,
        bucket_id: BucketId,
        create_ms: i64,
    ) -> Self {
        let base = InnerWriteBatch::new(batch_id, table_path, create_ms, bucket_id);

        Self {
            write_batch: base,
            arrow_builder: MemoryLogRecordsArrowBuilder::new(schema_id, row_type),
        }
    }

    pub fn batch_id(&self) -> i64 {
        self.write_batch.batch_id
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        if self.arrow_builder.is_closed() || self.arrow_builder.is_full() {
            Ok(None)
        } else {
            self.arrow_builder.append(&write_record.row)?;
            Ok(Some(ResultHandle::new(self.write_batch.results.receiver())))
        }
    }

    pub fn build(&self) -> Result<Vec<u8>> {
        self.arrow_builder.build()
    }

    pub fn is_closed(&self) -> bool {
        self.arrow_builder.is_closed()
    }

    pub fn close(&mut self) {
        self.arrow_builder.close()
    }
}
