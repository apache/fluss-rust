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

use crate::client::{WriteRecord, WriterClient};
use crate::row::GenericRow;
use std::sync::Arc;

use crate::error::Result;
use crate::metadata::{TableInfo, TablePath};

#[allow(dead_code)]
pub trait TableWriter {
    async fn flush(&self) -> Result<()>;
}

#[allow(dead_code)]
pub trait AppendWriter: TableWriter {
    async fn append(&self, row: GenericRow) -> Result<()>;
}

#[allow(dead_code)]
pub trait UpsertWriter: TableWriter {
    async fn upsert(&self, row: GenericRow) -> Result<()>;
    async fn delete(&self, row: GenericRow) -> Result<()>;
}

#[allow(dead_code)]
pub struct AbstractTableWriter {
    table_path: Arc<TablePath>,
    writer_client: Arc<WriterClient>,
    field_count: i32,
}

#[allow(dead_code)]
impl AbstractTableWriter {
    pub fn new(
        table_path: TablePath,
        table_info: &TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        // todo: partition
        Self {
            table_path: Arc::new(table_path),
            writer_client,
            field_count: table_info.row_type().fields().len() as i32,
        }
    }

    pub async fn send(&self, write_record: &WriteRecord<'_>) -> Result<()> {
        let result_handle = self.writer_client.send(write_record).await?;
        let result = result_handle.wait().await?;
        result_handle.result(result)
    }
}

impl TableWriter for AbstractTableWriter {
    async fn flush(&self) -> Result<()> {
        todo!()
    }
}

// Append writer implementation
#[allow(dead_code)]
pub struct AppendWriterImpl {
    base: AbstractTableWriter,
}

#[allow(dead_code)]
impl AppendWriterImpl {
    pub async fn append(&self, row: GenericRow<'_>) -> Result<()> {
        let record = WriteRecord::new(self.base.table_path.clone(), row);
        self.base.send(&record).await
    }
}
