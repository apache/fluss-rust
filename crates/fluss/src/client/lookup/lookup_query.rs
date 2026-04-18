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

use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TablePath};
use bytes::Bytes;
use tokio::sync::oneshot;

struct QueryShared {
    table_path: TablePath,
    table_bucket: TableBucket,
    key: Bytes,
    retries: i32,
}

pub struct PrimaryLookupQuery {
    shared: QueryShared,
    result_tx: Option<oneshot::Sender<Result<Option<Vec<u8>>>>>,
}

impl PrimaryLookupQuery {
    pub fn new(
        table_path: TablePath,
        table_bucket: TableBucket,
        key: Bytes,
        result_tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) -> Self {
        Self {
            shared: QueryShared {
                table_path,
                table_bucket,
                key,
                retries: 0,
            },
            result_tx: Some(result_tx),
        }
    }

    pub fn table_path(&self) -> &TablePath {
        &self.shared.table_path
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.shared.table_bucket
    }

    pub fn key(&self) -> &Bytes {
        &self.shared.key
    }

    pub fn retries(&self) -> i32 {
        self.shared.retries
    }

    pub fn increment_retries(&mut self) {
        self.shared.retries += 1;
    }

    pub fn is_done(&self) -> bool {
        self.result_tx.is_none()
    }

    pub fn complete(&mut self, result: Result<Option<Vec<u8>>>) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(result);
        }
    }

    pub fn complete_with_error(&mut self, error: Error) {
        self.complete(Err(error));
    }
}

impl From<PrimaryLookupQuery> for LookupQuery {
    fn from(q: PrimaryLookupQuery) -> Self {
        LookupQuery::Primary(q)
    }
}

pub struct PrefixLookupQuery {
    shared: QueryShared,
    result_tx: Option<oneshot::Sender<Result<Vec<Vec<u8>>>>>,
}

impl PrefixLookupQuery {
    pub fn new(
        table_path: TablePath,
        table_bucket: TableBucket,
        key: Bytes,
        result_tx: oneshot::Sender<Result<Vec<Vec<u8>>>>,
    ) -> Self {
        Self {
            shared: QueryShared {
                table_path,
                table_bucket,
                key,
                retries: 0,
            },
            result_tx: Some(result_tx),
        }
    }

    pub fn table_path(&self) -> &TablePath {
        &self.shared.table_path
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.shared.table_bucket
    }

    pub fn key(&self) -> &Bytes {
        &self.shared.key
    }

    pub fn retries(&self) -> i32 {
        self.shared.retries
    }

    pub fn increment_retries(&mut self) {
        self.shared.retries += 1;
    }

    pub fn is_done(&self) -> bool {
        self.result_tx.is_none()
    }

    pub fn complete(&mut self, result: Result<Vec<Vec<u8>>>) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(result);
        }
    }

    pub fn complete_with_error(&mut self, error: Error) {
        self.complete(Err(error));
    }
}

impl From<PrefixLookupQuery> for LookupQuery {
    fn from(q: PrefixLookupQuery) -> Self {
        LookupQuery::Prefix(q)
    }
}

pub enum LookupQuery {
    Primary(PrimaryLookupQuery),
    Prefix(PrefixLookupQuery),
}

impl LookupQuery {
    pub fn table_path(&self) -> &TablePath {
        match self {
            Self::Primary(q) => q.table_path(),
            Self::Prefix(q) => q.table_path(),
        }
    }

    pub fn table_bucket(&self) -> &TableBucket {
        match self {
            Self::Primary(q) => q.table_bucket(),
            Self::Prefix(q) => q.table_bucket(),
        }
    }

    pub fn key(&self) -> &Bytes {
        match self {
            Self::Primary(q) => q.key(),
            Self::Prefix(q) => q.key(),
        }
    }

    pub fn complete_with_error(&mut self, error: Error) {
        match self {
            Self::Primary(q) => q.complete_with_error(error),
            Self::Prefix(q) => q.complete_with_error(error),
        }
    }
}
