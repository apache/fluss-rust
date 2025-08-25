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
use crate::metadata::{TableInfo, TablePath};
use crate::row::GenericRow;
use std::sync::Arc;

use crate::error::Result;

#[allow(dead_code)]
pub struct TableAppend {
    table_path: TablePath,
    table_info: TableInfo,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub(super) fn new(
        table_path: TablePath,
        table_info: TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path,
            table_info,
            writer_client,
        }
    }

    pub fn create_writer(&self) -> AppendWriter {
        AppendWriter {
            table_path: Arc::new(self.table_path.clone()),
            writer_client: self.writer_client.clone(),
        }
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    writer_client: Arc<WriterClient>,
}

impl AppendWriter {
    pub async fn append(&self, row: GenericRow<'_>) -> Result<()> {
        let record = WriteRecord::new(self.table_path.clone(), row);
        let result_handle = self.writer_client.send(&record).await?;
        let result = result_handle.wait().await?;
        result_handle.result(result)
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}
