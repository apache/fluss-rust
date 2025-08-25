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

use crate::client::broadcast::{BatchWriteResult, BroadcastOnceReceiver};
use crate::error::Error;
use crate::metadata::TablePath;
use crate::row::GenericRow;
pub use accumulator::*;
use std::sync::Arc;

pub(crate) mod broadcast;
mod bucket_assigner;

mod sender;
mod writer_client;

pub use writer_client::WriterClient;

pub struct WriteRecord<'a> {
    pub row: GenericRow<'a>,
    pub table_path: Arc<TablePath>,
}

impl<'a> WriteRecord<'a> {
    pub fn new(table_path: Arc<TablePath>, row: GenericRow<'a>) -> Self {
        Self { row, table_path }
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
            .map_err(|e| Error::WriteError(e.to_string()))
    }

    pub fn result(&self, batch_result: BatchWriteResult) -> Result<(), Error> {
        // do nothing, just return empty result
        batch_result.map_err(|e| Error::WriteError(e.to_string()))
    }
}
