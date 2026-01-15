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

use crate::bucketing::BucketingFunction;
use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TableInfo};
use crate::rpc::ApiError;
use crate::rpc::message::LookupRequest;
use std::sync::Arc;

/// Configuration and factory struct for creating lookup operations.
///
/// `TableLookup` follows the same pattern as `TableScan` and `TableAppend`,
/// providing a builder-style API for configuring lookup operations before
/// creating the actual `Lookuper`.
///
/// # Example
/// ```ignore
/// let table = conn.get_table(&table_path).await?;
/// let lookuper = table.new_lookup()?.create_lookuper()?;
/// let value = lookuper.lookup(encoded_key).await?;
/// ```
// TODO: Add lookup_by(column_names) for prefix key lookups (PrefixKeyLookuper)
// TODO: Add create_typed_lookuper<T>() for typed lookups with POJO mapping
pub struct TableLookup<'a> {
    conn: &'a FlussConnection,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
}

impl<'a> TableLookup<'a> {
    pub(super) fn new(
        conn: &'a FlussConnection,
        table_info: TableInfo,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            conn,
            table_info,
            metadata,
        }
    }

    /// Creates a `Lookuper` for performing key-based lookups.
    ///
    /// The lookuper will automatically compute the bucket for each key
    /// using the appropriate bucketing function.
    pub fn create_lookuper(self) -> Result<Lookuper<'a>> {
        let num_buckets = self.table_info.get_num_buckets();
        let bucketing_function = <dyn BucketingFunction>::of(None);

        Ok(Lookuper {
            conn: self.conn,
            table_info: self.table_info,
            metadata: self.metadata,
            bucketing_function,
            num_buckets,
        })
    }
}

/// Performs key-based lookups against a primary key table.
///
/// The `Lookuper` automatically computes the target bucket from the key,
/// finds the appropriate tablet server, and retrieves the value.
///
/// # Example
/// ```ignore
/// let lookuper = table.new_lookup()?.create_lookuper()?;
/// let key = vec![1, 2, 3]; // encoded primary key bytes
/// if let Some(value) = lookuper.lookup(key).await? {
///     println!("Found value: {:?}", value);
/// }
/// ```
// TODO: Support partitioned tables (extract partition from key)
// TODO: Detect data lake format from table config for bucketing function
pub struct Lookuper<'a> {
    conn: &'a FlussConnection,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    bucketing_function: Box<dyn BucketingFunction>,
    num_buckets: i32,
}

impl<'a> Lookuper<'a> {
    /// Looks up a value by its primary key.
    ///
    /// The bucket is automatically computed from the key using the table's
    /// bucketing function.
    ///
    /// # Arguments
    /// * `key` - The encoded primary key bytes
    ///
    /// # Returns
    /// * `Ok(Some(Vec<u8>))` - The value bytes if the key exists
    /// * `Ok(None)` - If the key does not exist
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // Compute bucket from key
        let bucket_id = self.bucketing_function.bucketing(&key, self.num_buckets)?;

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
