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
use crate::client::lookup::LookupClient;
use crate::client::metadata::Metadata;
use crate::client::table::partition_getter::PartitionGetter;
use crate::error::{Error, Result};
use crate::metadata::{PhysicalTablePath, RowType, TableBucket, TableInfo, TablePath};
use crate::record::RowAppendRecordBatchBuilder;
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::InternalRow;
use crate::row::compacted::CompactedRow;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use arrow::array::RecordBatch;
use std::sync::Arc;

/// The result of a lookup operation.
///
/// Contains the rows returned from a lookup. For primary key lookups,
/// this will contain at most one row. For prefix key lookups (future),
/// this may contain multiple rows.
pub struct LookupResult {
    rows: Vec<Vec<u8>>,
    row_type: Arc<RowType>,
}

impl LookupResult {
    /// Creates a new LookupResult from a list of row bytes.
    fn new(rows: Vec<Vec<u8>>, row_type: Arc<RowType>) -> Self {
        Self { rows, row_type }
    }

    /// Creates an empty LookupResult.
    fn empty(row_type: Arc<RowType>) -> Self {
        Self {
            rows: Vec::new(),
            row_type,
        }
    }

    /// Extracts the row payload by stripping the schema id prefix.
    fn extract_payload(bytes: &[u8]) -> Result<&[u8]> {
        bytes
            .get(SCHEMA_ID_LENGTH..)
            .ok_or_else(|| Error::RowConvertError {
                message: format!(
                    "Row payload too short: {} bytes, need at least {} for schema id",
                    bytes.len(),
                    SCHEMA_ID_LENGTH
                ),
            })
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
    /// - `Err(Error)`: If the row payload is too short to contain a schema id.
    pub fn get_single_row(&self) -> Result<Option<CompactedRow<'_>>> {
        match self.rows.len() {
            0 => Ok(None),
            1 => {
                let payload = Self::extract_payload(&self.rows[0])?;
                Ok(Some(CompactedRow::from_bytes(&self.row_type, payload)))
            }
            _ => Err(Error::UnexpectedError {
                message: "LookupResult contains multiple rows, use get_rows() instead".to_string(),
                source: None,
            }),
        }
    }

    /// Returns all rows in the result set as [`CompactedRow`]s.
    ///
    /// # Returns
    /// - `Ok(rows)` - All rows in the result set.
    /// - `Err(Error)` - If any row payload is too short to contain a schema id.
    pub fn get_rows(&self) -> Result<Vec<CompactedRow<'_>>> {
        self.rows
            .iter()
            // TODO Add schema id check and fetch when implementing prefix lookup
            .map(|bytes| {
                let payload = Self::extract_payload(bytes)?;
                Ok(CompactedRow::from_bytes(&self.row_type, payload))
            })
            .collect()
    }

    /// Converts all rows in this result into an Arrow [`RecordBatch`].
    ///
    /// This is useful for integration with DataFusion or other Arrow-based tools.
    ///
    /// # Returns
    /// - `Ok(RecordBatch)` - All rows in columnar Arrow format. Returns an empty
    ///   batch (with the correct schema) if the result set is empty.
    /// - `Err(Error)` - If the conversion fails.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut builder = RowAppendRecordBatchBuilder::new(&self.row_type)?;

        for bytes in &self.rows {
            let payload = Self::extract_payload(bytes)?;

            let row = CompactedRow::from_bytes(&self.row_type, payload);
            builder.append(&row)?;
        }

        builder.build_arrow_record_batch().map(Arc::unwrap_or_clone)
    }
}

/// Builder for lookup operations. `create_lookuper()` builds a primary-key
/// `Lookuper`; `lookup_by(columns).create_lookuper()` builds a
/// `PrefixKeyLookuper` for prefix scans.
// TODO: Add create_typed_lookuper<T>() for typed lookups with POJO mapping
pub struct TableLookup {
    lookup_client: Arc<LookupClient>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
}

impl TableLookup {
    pub(super) fn new(
        lookup_client: Arc<LookupClient>,
        table_info: TableInfo,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            lookup_client,
            table_info,
            metadata,
        }
    }

    /// Switches the builder into prefix-scan mode. `lookup_column_names`
    /// must list the table's partition keys (if any) plus the bucket keys,
    /// in that order — i.e. this is a **bucket-key prefix** scan, not an
    /// arbitrary primary-key prefix. Validation is deferred to
    /// `create_lookuper()`.
    pub fn lookup_by(self, lookup_column_names: Vec<String>) -> TablePrefixLookup {
        TablePrefixLookup {
            lookup_client: self.lookup_client,
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_column_names,
        }
    }

    /// Creates a `Lookuper` for performing key-based lookups.
    ///
    /// The lookuper will automatically encode the key and compute the bucket
    /// for each lookup using the appropriate bucketing function.
    ///
    /// The lookuper uses a shared `LookupClient` that batches multiple lookup
    /// operations together to reduce network round trips. This achieves parity
    /// with the Java client implementation for improved throughput.
    pub fn create_lookuper(self) -> Result<Lookuper> {
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

        let row_type = Arc::new(self.table_info.row_type().clone());

        Ok(Lookuper {
            table_path: Arc::new(self.table_info.table_path.clone()),
            row_type,
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_client: self.lookup_client,
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
/// bucket, and retrieves the value using the batched `LookupClient`.
///
/// # Example
/// ```ignore
/// let lookuper = table.new_lookup()?.create_lookuper()?;
/// let row = GenericRow::new(vec![Datum::Int32(42)]); // lookup key
/// let result = lookuper.lookup(&row).await?;
/// ```
pub struct Lookuper {
    table_path: Arc<TablePath>,
    table_info: TableInfo,
    row_type: Arc<RowType>,
    metadata: Arc<Metadata>,
    lookup_client: Arc<LookupClient>,
    bucketing_function: Box<dyn BucketingFunction>,
    primary_key_encoder: Box<dyn KeyEncoder>,
    bucket_key_encoder: Option<Box<dyn KeyEncoder>>,
    partition_getter: Option<PartitionGetter>,
    num_buckets: i32,
}

impl Lookuper {
    /// Looks up a value by its primary key.
    ///
    /// The key is encoded and the bucket is automatically computed using
    /// the table's bucketing function. The lookup is queued and batched
    /// with other lookups for improved throughput.
    ///
    /// # Arguments
    /// * `row` - The row containing the primary key field values
    ///
    /// # Returns
    /// * `Ok(LookupResult)` - The lookup result (may be empty if key not found)
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(&mut self, row: &dyn InternalRow) -> Result<LookupResult> {
        let pk_bytes = self.primary_key_encoder.encode_key(row)?;
        let bk_bytes = match &mut self.bucket_key_encoder {
            Some(encoder) => encoder.encode_key(row)?,
            None => pk_bytes.clone(),
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
                    return Ok(LookupResult::empty(Arc::clone(&self.row_type)));
                }
            }
        } else {
            None
        };

        let bucket_id = self
            .bucketing_function
            .bucketing(&bk_bytes, self.num_buckets)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        // Use the batched lookup client
        let result = self
            .lookup_client
            .lookup(self.table_path.as_ref().clone(), table_bucket, pk_bytes)
            .await?;

        match result {
            Some(value_bytes) => Ok(LookupResult::new(
                vec![value_bytes],
                Arc::clone(&self.row_type),
            )),
            None => Ok(LookupResult::empty(Arc::clone(&self.row_type))),
        }
    }

    /// Returns a reference to the table info.
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

pub struct TablePrefixLookup {
    lookup_client: Arc<LookupClient>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    lookup_column_names: Vec<String>,
}

impl TablePrefixLookup {
    pub fn create_lookuper(self) -> Result<PrefixKeyLookuper> {
        validate_prefix_lookup(&self.table_info, &self.lookup_column_names)?;

        let num_buckets = self.table_info.get_num_buckets();
        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let lookup_row_type = row_type.project_with_field_names(&self.lookup_column_names)?;

        let bucket_keys = self.table_info.get_bucket_keys().to_vec();
        let prefix_key_encoder =
            KeyEncoderFactory::of(&lookup_row_type, &bucket_keys, &data_lake_format)?;

        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                &lookup_row_type,
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let full_row_type = Arc::new(self.table_info.row_type().clone());

        Ok(PrefixKeyLookuper {
            table_path: Arc::new(self.table_info.table_path.clone()),
            row_type: full_row_type,
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_client: self.lookup_client,
            bucketing_function,
            prefix_key_encoder,
            partition_getter,
            num_buckets,
        })
    }
}

fn validate_prefix_lookup(table_info: &TableInfo, lookup_columns: &[String]) -> Result<()> {
    if !table_info.has_primary_key() {
        return Err(Error::IllegalArgument {
            message: format!(
                "Log table {} doesn't support prefix lookup",
                table_info.get_table_path()
            ),
        });
    }

    let physical_primary_keys = table_info.get_physical_primary_keys();
    let bucket_keys = table_info.get_bucket_keys();

    if bucket_keys.is_empty() {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because it has no bucket keys.",
                table_info.get_table_path()
            ),
        });
    }

    for (i, bucket_key) in bucket_keys.iter().enumerate() {
        let pk = physical_primary_keys
            .get(i)
            .ok_or_else(|| Error::IllegalArgument {
                message: format!(
                    "Can not perform prefix lookup on table '{}', because the bucket keys {:?} \
                 is not a prefix subset of the physical primary keys {:?} \
                 (excluded partition fields if present).",
                    table_info.get_table_path(),
                    bucket_keys,
                    physical_primary_keys,
                ),
            })?;
        if bucket_key != pk {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Can not perform prefix lookup on table '{}', because the bucket keys {:?} \
                     is not a prefix subset of the physical primary keys {:?} \
                     (excluded partition fields if present).",
                    table_info.get_table_path(),
                    bucket_keys,
                    physical_primary_keys,
                ),
            });
        }
    }

    let partition_keys: &[String] = table_info.get_partition_keys();
    if table_info.is_partitioned() {
        for pk in partition_keys {
            if !lookup_columns.iter().any(|c| c == pk) {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Can not perform prefix lookup on table '{}', because the lookup columns \
                         {:?} must contain all partition fields {:?}.",
                        table_info.get_table_path(),
                        lookup_columns,
                        partition_keys,
                    ),
                });
            }
        }
    }

    let physical_lookup_columns: Vec<&String> = lookup_columns
        .iter()
        .filter(|c| !partition_keys.iter().any(|p| p == *c))
        .collect();
    if physical_lookup_columns.len() != bucket_keys.len()
        || !physical_lookup_columns
            .iter()
            .zip(bucket_keys.iter())
            .all(|(a, b)| *a == b)
    {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because the lookup columns {:?} \
                 must contain all bucket keys {:?} in order.",
                table_info.get_table_path(),
                lookup_columns,
                bucket_keys,
            ),
        });
    }

    if bucket_keys == physical_primary_keys {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because the lookup columns {:?} \
                 equals the physical primary keys {:?}. \
                 Please use primary key lookup (Lookuper without lookup_by) instead.",
                table_info.get_table_path(),
                lookup_columns,
                physical_primary_keys,
            ),
        });
    }

    Ok(())
}

pub struct PrefixKeyLookuper {
    table_path: Arc<TablePath>,
    table_info: TableInfo,
    row_type: Arc<RowType>,
    metadata: Arc<Metadata>,
    lookup_client: Arc<LookupClient>,
    bucketing_function: Box<dyn BucketingFunction>,
    prefix_key_encoder: Box<dyn KeyEncoder>,
    partition_getter: Option<PartitionGetter>,
    num_buckets: i32,
}

impl PrefixKeyLookuper {
    pub async fn lookup(&mut self, row: &dyn InternalRow) -> Result<LookupResult> {
        let prefix_bytes = self.prefix_key_encoder.encode_key(row)?;

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
                    return Ok(LookupResult::empty(Arc::clone(&self.row_type)));
                }
            }
        } else {
            None
        };

        let bucket_id = self
            .bucketing_function
            .bucketing(&prefix_bytes, self.num_buckets)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        let rows = self
            .lookup_client
            .prefix_lookup(self.table_path.as_ref().clone(), table_bucket, prefix_bytes)
            .await?;

        Ok(LookupResult::new(rows, Arc::clone(&self.row_type)))
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;
    use arrow::array::Int32Array;

    fn make_row_bytes(schema_id: i16, row_data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(SCHEMA_ID_LENGTH + row_data.len());
        bytes.extend_from_slice(&schema_id.to_le_bytes());
        bytes.extend_from_slice(row_data);
        bytes
    }

    #[test]
    fn test_to_record_batch_empty() {
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "id",
            DataTypes::int(),
            None,
        )]));
        let result = LookupResult::empty(row_type);
        let batch = result.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_to_record_batch_with_row() {
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "id",
            DataTypes::int(),
            None,
        )]));

        let mut writer = CompactedRowWriter::new(1);
        writer.write_int(42);
        let row_bytes = make_row_bytes(0, writer.buffer());

        let result = LookupResult::new(vec![row_bytes], Arc::clone(&row_type));
        let batch = result.to_record_batch().unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn test_to_record_batch_payload_too_short() {
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "id",
            DataTypes::int(),
            None,
        )]));
        // Only 1 byte — shorter than SCHEMA_ID_LENGTH (2)
        let result = LookupResult::new(vec![vec![0u8]], Arc::clone(&row_type));
        assert!(result.to_record_batch().is_err());
    }
}
