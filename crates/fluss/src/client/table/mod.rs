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

use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TableInfo, TablePath};
use crate::rpc::ApiError;
use crate::rpc::message::LookupRequest;
use std::sync::Arc;

pub const EARLIEST_OFFSET: i64 = -2;

mod append;

mod log_fetch_buffer;
mod remote_log;
mod scanner;
mod writer;

pub use append::{AppendWriter, TableAppend};
pub use scanner::{LogScanner, RecordBatchLogScanner, TableScan};

#[allow(dead_code)]
pub struct FlussTable<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl<'a> FlussTable<'a> {
    pub fn new(conn: &'a FlussConnection, metadata: Arc<Metadata>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
            metadata,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_append(&self) -> Result<TableAppend> {
        Ok(TableAppend::new(
            self.table_path.clone(),
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> TableScan<'_> {
        TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone())
    }

    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    /// Lookup values by primary key in a key-value table.
    ///
    /// This method performs a direct lookup to retrieve the value associated with the given key
    /// in the specified bucket. The table must have a primary key (be a primary key table).
    ///
    /// # Arguments
    /// * `bucket_id` - The bucket ID to look up the key in
    /// * `key` - The encoded primary key bytes to look up
    ///
    /// # Returns
    /// * `Ok(Some(Vec<u8>))` - The value bytes if the key exists
    /// * `Ok(None)` - If the key does not exist
    /// * `Err(Error)` - If the lookup fails or the table doesn't have a primary key
    ///
    /// # Example
    /// ```ignore
    /// let table = conn.get_table(&table_path).await?;
    /// let key = /* encoded key bytes */;
    /// if let Some(value) = table.lookup(0, key).await? {
    ///     println!("Found value: {:?}", value);
    /// }
    /// ```
    pub async fn lookup(&self, bucket_id: i32, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if !self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Lookup is only supported for primary key tables".to_string(),
            });
        }

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new(table_id, bucket_id);

        // Find the leader for this bucket
        let cluster = self.metadata.get_cluster();
        let leader =
            cluster
                .leader_for(&table_bucket)
                .ok_or_else(|| Error::LeaderNotAvailable {
                    message: format!("No leader found for table bucket: {table_bucket}"),
                })?;

        // Get connection to the tablet server
        let tablet_server =
            cluster
                .get_tablet_server(leader.id())
                .ok_or_else(|| Error::LeaderNotAvailable {
                    message: format!(
                        "Tablet server {} is not found in metadata cache",
                        leader.id()
                    ),
                })?;

        let connections = self.conn.get_connections();
        let connection = connections.get_connection(tablet_server).await?;

        // Send lookup request
        let request = LookupRequest::new(table_id, None, bucket_id, vec![key]);
        let response = connection.request(request).await?;

        // Extract the value from response
        if let Some(bucket_resp) = response.buckets_resp.into_iter().next() {
            // Check for errors
            if let Some(error_code) = bucket_resp.error_code {
                if error_code != 0 {
                    return Err(Error::FlussAPIError {
                        api_error: ApiError {
                            code: error_code,
                            message: bucket_resp.error_message.unwrap_or_default(),
                        },
                    });
                }
            }

            // Get the first value (we only requested one key)
            if let Some(pb_value) = bucket_resp.values.into_iter().next() {
                return Ok(pb_value.values);
            }
        }

        Ok(None)
    }
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}
