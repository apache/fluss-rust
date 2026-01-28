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

use crate::row::{GenericRow, InternalRow};

use crate::error::Result;

#[allow(dead_code, async_fn_in_trait)]
pub trait TableWriter {
    async fn flush(&self) -> Result<()>;
}

#[allow(dead_code)]
pub trait AppendWriter: TableWriter {
    async fn append(&self, row: GenericRow) -> Result<()>;
}

#[allow(dead_code, async_fn_in_trait)]
pub trait UpsertWriter: TableWriter {
    async fn upsert<R: InternalRow>(&mut self, row: &R) -> Result<UpsertResult>;
    async fn delete<R: InternalRow>(&mut self, row: &R) -> Result<DeleteResult>;
}

/// The result of upserting a record
/// Currently this is an empty struct to allow for compatible evolution in the future
#[derive(Default)]
pub struct UpsertResult;

/// The result of deleting a record
/// Currently this is an empty struct to allow for compatible evolution in the future
#[derive(Default)]
pub struct DeleteResult;
