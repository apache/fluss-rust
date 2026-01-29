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
use crate::client::table::partition_getter::PartitionGetter;
use crate::error::{Error, Result};
use crate::metadata::{PhysicalTablePath, RowType, TableBucket, TableInfo, TablePath};
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::InternalRow;
use crate::row::compacted::CompactedRow;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::rpc::ApiError;
use crate::rpc::message::LookupRequest;
use std::sync::Arc;

/// The result of a lookup operation.
///
/// Contains the rows returned from a lookup. For primary key lookups,
/// this will contain at most one row. For prefix key lookups (future),
/// this may contain multiple rows.
pub struct LookupResult<'a> {
    rows: Vec<Vec<u8>>,
    row_type: &'a RowType,
}

impl<'a> LookupResult<'a> {
    /// Creates a new LookupResult from a list of row bytes.
    fn new(rows: Vec<Vec<u8>>, row_type: &'a RowType) -> Self {
        Self { rows, row_type }
    }

    /// Creates an empty LookupResult.
    fn empty(row_type: &'a RowType) -> Self {
        Self {
            rows: Vec::new(),
            row_type,
        }
    }

    /// Returns the only row in the result set as a [`CompactedRow`].
    ///
    /// This method provides a zero-copy view of the row data, which means the returned
    /// `CompactedRow` borrows from this result set and cannot outlive it.
    ///
    /// # Returns
    /// - `Ok(Some(row))`: If exactly one row exists.
    /// - `Ok(None)`: If the result set is empty.
    /// - `Err(Error::UnexpectedError)`: If the result set contains more than one row.
    ///
    pub fn get_single_row(&self) -> Result<Option<CompactedRow<'_>>> {
        match self.rows.len() {
            0 => Ok(None),
            1 => Ok(Some(CompactedRow::from_bytes(
                self.row_type,
                &self.rows[0][SCHEMA_ID_LENGTH..],
            ))),
            _ => Err(Error::UnexpectedError {
                message: "LookupResult contains multiple rows, use get_rows() instead".to_string(),
                source: None,
            }),
        }
    }

    /// Returns all rows as CompactedRows.
    pub fn get_rows(&self) -> Vec<CompactedRow<'_>> {
        self.rows
            .iter()
            // TODO Add schema id check and fetch when implementing prefix lookup
            .map(|bytes| CompactedRow::from_bytes(self.row_type, &bytes[SCHEMA_ID_LENGTH..]))
            .collect()
    }
}

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
/// let result = lookuper.lookup(&row).await?;
/// if let Some(value) = result.get_single_row() {
///     println!("Found: {:?}", value);
/// }
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
    /// The lookuper will automatically encode the key and compute the bucket
    /// for each lookup using the appropriate bucketing function.
    pub fn create_lookuper(self) -> Result<Lookuper<'a>> {
        let num_buckets = self.table_info.get_num_buckets();

        // Get data lake format from table config for bucketing function
        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let primary_keys = self.table_info.get_primary_keys();
        let lookup_row_type = row_type.project_with_field_names(primary_keys)?;

        let physical_primary_keys = self.table_info.get_physical_primary_keys().to_vec();
        let primary_key_encoder =
            KeyEncoderFactory::of(&lookup_row_type, &physical_primary_keys, &data_lake_format)?;

        let bucket_key_encoder = if self.table_info.is_default_bucket_key() {
            None
        } else {
            let bucket_keys = self.table_info.get_bucket_keys().to_vec();
            Some(KeyEncoderFactory::of(
                &lookup_row_type,
                &bucket_keys,
                &data_lake_format,
            )?)
        };

        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                &lookup_row_type,
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        Ok(Lookuper {
            conn: self.conn,
            table_path: Arc::new(self.table_info.table_path.clone()),
            table_info: self.table_info,
            metadata: self.metadata,
            bucketing_function,
            primary_key_encoder,
            bucket_key_encoder,
            partition_getter,
            num_buckets,
        })
    }
}

/// Performs key-based lookups against a primary key table.
///
/// The `Lookuper` automatically encodes the lookup key, computes the target
/// bucket, finds the appropriate tablet server, and retrieves the value.
///
/// # Example
/// ```ignore
/// let lookuper = table.new_lookup()?.create_lookuper()?;
/// let row = GenericRow::new(vec![Datum::Int32(42)]); // lookup key
/// let result = lookuper.lookup(&row).await?;
/// ```
pub struct Lookuper<'a> {
    conn: &'a FlussConnection,
    table_info: TableInfo,
    table_path: Arc<TablePath>,
    metadata: Arc<Metadata>,
    bucketing_function: Box<dyn BucketingFunction>,
    primary_key_encoder: Box<dyn KeyEncoder>,
    bucket_key_encoder: Option<Box<dyn KeyEncoder>>,
    partition_getter: Option<PartitionGetter>,
    num_buckets: i32,
}

impl<'a> Lookuper<'a> {
    /// Looks up a value by its primary key.
    ///
    /// The key is encoded and the bucket is automatically computed using
    /// the table's bucketing function.
    ///
    /// # Arguments
    /// * `row` - The row containing the primary key field values
    ///
    /// # Returns
    /// * `Ok(LookupResult)` - The lookup result (may be empty if key not found)
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(&mut self, row: &dyn InternalRow) -> Result<LookupResult<'_>> {
        // todo: support batch lookup
        let pk_bytes = self.primary_key_encoder.encode_key(row)?;
        let pk_bytes_vec = pk_bytes.to_vec();
        let bk_bytes = match &mut self.bucket_key_encoder {
            Some(encoder) => &encoder.encode_key(row)?,
            None => &pk_bytes,
        };

        let partition_id = if let Some(ref partition_getter) = self.partition_getter {
            let partition_name = partition_getter.get_partition(row)?;
            let physical_table_path = PhysicalTablePath::of_partitioned(
                Arc::clone(&self.table_path),
                Some(partition_name),
            );
            let cluster = self.metadata.get_cluster();
            match cluster.get_partition_id(&physical_table_path) {
                Some(id) => Some(id),
                None => {
                    // Partition doesn't exist, return empty result (like Java)
                    return Ok(LookupResult::empty(self.table_info.row_type()));
                }
            }
        } else {
            None
        };

        let bucket_id = self
            .bucketing_function
            .bucketing(bk_bytes, self.num_buckets)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        // Find the leader for this bucket
        let cluster = self.metadata.get_cluster();
        let leader = self
            .metadata
            .leader_for(self.table_info.get_table_path(), &table_bucket)
            .await?
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
        let request = LookupRequest::new(table_id, partition_id, bucket_id, vec![pk_bytes_vec]);
        let response = connection.request(request).await?;

        // Extract the values from response
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

            // Collect all values
            let rows: Vec<Vec<u8>> = bucket_resp
                .values
                .into_iter()
                .filter_map(|pb_value| pb_value.values)
                .collect();

            return Ok(LookupResult::new(rows, self.table_info.row_type()));
        }

        Ok(LookupResult::empty(self.table_info.row_type()))
    }

    /// Returns a reference to the table info.
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}
