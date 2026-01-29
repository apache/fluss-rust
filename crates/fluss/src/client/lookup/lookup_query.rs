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

//! Lookup query representation for batching lookup operations.

use crate::metadata::{TableBucket, TablePath};
use std::sync::atomic::{AtomicI32, Ordering};
use tokio::sync::oneshot;

/// Represents a single lookup query that will be batched and sent to the server.
pub struct LookupQuery {
    /// The table path for this lookup
    table_path: TablePath,
    /// The table bucket for this lookup
    table_bucket: TableBucket,
    /// The encoded primary key bytes
    key: Vec<u8>,
    /// Channel to send the result back to the caller
    result_tx: Option<oneshot::Sender<Result<Option<Vec<u8>>, crate::error::Error>>>,
    /// Number of retry attempts
    retries: AtomicI32,
}

impl LookupQuery {
    /// Creates a new lookup query.
    pub fn new(
        table_path: TablePath,
        table_bucket: TableBucket,
        key: Vec<u8>,
        result_tx: oneshot::Sender<Result<Option<Vec<u8>>, crate::error::Error>>,
    ) -> Self {
        Self {
            table_path,
            table_bucket,
            key,
            result_tx: Some(result_tx),
            retries: AtomicI32::new(0),
        }
    }

    /// Returns the table path.
    #[allow(dead_code)]
    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    /// Returns the table bucket.
    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    /// Returns the encoded key bytes.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the current retry count.
    pub fn retries(&self) -> i32 {
        self.retries.load(Ordering::Acquire)
    }

    /// Increments the retry counter.
    pub fn increment_retries(&self) {
        self.retries.fetch_add(1, Ordering::AcqRel);
    }

    /// Completes the lookup with a result.
    pub fn complete(&mut self, result: Result<Option<Vec<u8>>, crate::error::Error>) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(result);
        }
    }

    /// Returns true if the result has already been sent.
    pub fn is_done(&self) -> bool {
        self.result_tx.is_none()
    }
}
