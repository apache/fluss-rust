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

//! One-shot bounded scanner backed by a single `LimitScanRequest` RPC.
//!
//! Unlike [`crate::client::table::LogScanner`], a `BatchScanner` does not
//! subscribe to bucket offsets or stream from the server. It performs a single
//! eager request for up to `limit` rows from one `TableBucket` and exposes the
//! result as a single Arrow [`RecordBatch`] on the first call to
//! [`BatchScanner::poll_batch`]; subsequent calls return `None`.

use crate::client::metadata::Metadata;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{TableBucket, TableInfo};
use crate::proto::ErrorResponse;
use crate::record::kv::{KvRecordBatch, KvRecordReadContext, ReadContext as KvReadContext, SchemaGetter};
use crate::record::{LogRecordsBatches, ReadContext as ArrowReadContext, ScanBatch, RowAppendRecordBatchBuilder, to_arrow_schema};
use crate::rpc::RpcClient;
use crate::rpc::message::LimitScanRequest;
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use std::sync::Arc;

/// Adapter over a [`TableInfo`] that satisfies [`SchemaGetter`] for a single
/// table. KV lookups always carry the same schema id, so we just hand back
/// the embedded schema.
struct TableInfoSchemaGetter {
    schema: Arc<crate::metadata::Schema>,
}

impl SchemaGetter for TableInfoSchemaGetter {
    fn get_schema(&self, _schema_id: i16) -> Result<Arc<crate::metadata::Schema>> {
        Ok(Arc::clone(&self.schema))
    }
}

/// One-shot bounded scanner.
///
/// The scanner sends a single `LimitScanRequest` on construction and caches
/// the resulting Arrow `RecordBatch`. The first `poll_batch()` returns the
/// batch (wrapped in a [`ScanBatch`]); the second returns `None`.
pub struct BatchScanner {
    bucket: TableBucket,
    /// Pre-fetched batch, taken out on the first `poll_batch` call.
    batch: Option<RecordBatch>,
}

impl BatchScanner {
    pub(super) async fn new(
        rpc_client: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        table_info: TableInfo,
        projected_fields: Option<Vec<usize>>,
        bucket: TableBucket,
        limit: i32,
    ) -> Result<Self> {
        // Resolve leader for the target bucket (mirrors Lookuper's pattern).
        let leader = metadata
            .leader_for(&table_info.table_path, &bucket)
            .await?
            .ok_or_else(|| {
                Error::leader_not_available(format!(
                    "No leader found for table bucket: {bucket}"
                ))
            })?;
        let connection = rpc_client.get_connection(&leader).await?;

        // Fire the single LimitScanRequest RPC.
        let request = LimitScanRequest::new(
            table_info.table_id,
            bucket.partition_id(),
            bucket.bucket_id(),
            limit,
        );
        let response = connection.request(request).await?;

        // Surface server-side errors using the same shape as Lookuper.
        if let Some(error_code) = response.error_code
            && error_code != FlussError::None.code()
        {
            let err: ApiError = ErrorResponse {
                error_code,
                error_message: response.error_message.clone(),
            }
            .into();
            return Err(Error::FlussAPIError { api_error: err });
        }

        let is_log_table = response.is_log_table.unwrap_or(false);
        let raw = response.records.unwrap_or_default();

        let batch = if is_log_table {
            decode_log_batch(&table_info, projected_fields.as_deref(), raw)?
        } else {
            decode_kv_batch(&table_info, projected_fields.as_deref(), raw)?
        };

        Ok(Self {
            bucket,
            batch: Some(batch),
        })
    }

    /// Returns the pre-fetched batch on the first call, then `None`.
    pub async fn poll_batch(&mut self) -> Result<Option<ScanBatch>> {
        Ok(self
            .batch
            .take()
            .map(|b| ScanBatch::new(self.bucket.clone(), b, 0)))
    }

    /// The bucket scanned by this `BatchScanner`.
    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }
}

/// Decode an Arrow-IPC encoded `LogRecordBatch` payload into a single Arrow
/// `RecordBatch`. Multiple inner batches (rare for a `LimitScanRequest`) are
/// concatenated.
fn decode_log_batch(
    table_info: &TableInfo,
    projected_fields: Option<&[usize]>,
    raw: Vec<u8>,
) -> Result<RecordBatch> {
    let full_schema = to_arrow_schema(table_info.get_row_type())?;
    let read_context = match projected_fields {
        None => ArrowReadContext::new(full_schema.clone(), false),
        Some(fields) => {
            ArrowReadContext::with_projection_pushdown(full_schema.clone(), fields.to_vec(), false)?
        }
    };

    let target_schema: SchemaRef = match projected_fields {
        None => full_schema,
        Some(fields) => ArrowReadContext::project_schema(
            to_arrow_schema(table_info.get_row_type())?,
            fields,
        )?,
    };

    if raw.is_empty() {
        return Ok(RecordBatch::new_empty(target_schema));
    }

    let mut batches: Vec<RecordBatch> = Vec::new();
    for log_batch in LogRecordsBatches::new(raw) {
        let log_batch = log_batch?;
        let rb = log_batch.record_batch(&read_context)?;
        batches.push(rb);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(target_schema));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    arrow::compute::concat_batches(&target_schema, batches.iter()).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to concatenate log record batches: {e}"),
            source: None,
        }
    })
}

/// Decode a KV-format payload into a single Arrow `RecordBatch`. Each
/// `CompactedRow` is appended through [`RowAppendRecordBatchBuilder`]; deletion
/// records (no value) are skipped because primary key tables don't return
/// tombstones from a limit scan.
fn decode_kv_batch(
    table_info: &TableInfo,
    projected_fields: Option<&[usize]>,
    raw: Vec<u8>,
) -> Result<RecordBatch> {
    let row_type = table_info.get_row_type();
    let full_arrow_schema = to_arrow_schema(row_type)?;

    if raw.is_empty() {
        let schema: SchemaRef = match projected_fields {
            None => full_arrow_schema,
            Some(fields) => ArrowReadContext::project_schema(full_arrow_schema, fields)?,
        };
        return Ok(RecordBatch::new_empty(schema));
    }

    let kv_format = table_info.table_config.get_kv_format()?;
    let schema_getter = Arc::new(TableInfoSchemaGetter {
        schema: Arc::new(table_info.get_schema().clone()),
    });
    let read_context = KvRecordReadContext::new(kv_format, schema_getter);

    // The KV records payload may be a single batch or a sequence of batches.
    // The server-side `LimitScanResponse` returns one batch in practice, but
    // we walk the buffer defensively.
    let bytes = Bytes::from(raw);
    let mut builder = RowAppendRecordBatchBuilder::new(row_type)?;
    let mut position = 0usize;

    while position < bytes.len() {
        let kv_batch = KvRecordBatch::new(bytes.clone(), position);
        let size = kv_batch.size_in_bytes().map_err(|e| Error::UnexpectedError {
            message: format!("Invalid KvRecordBatch length: {e}"),
            source: None,
        })?;

        let records = kv_batch.records_unchecked(&read_context as &dyn KvReadContext)?;
        let decoder = records.decoder_arc();
        for record in records {
            let record = record.map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read KV record: {e}"),
                source: None,
            })?;
            if let Some(row) = record.row(&*decoder) {
                builder.append(&row)?;
            }
        }

        position = position.checked_add(size).ok_or_else(|| Error::UnexpectedError {
            message: "KvRecordBatch position overflow".to_string(),
            source: None,
        })?;
    }

    let full_batch = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);

    match projected_fields {
        None => Ok(full_batch),
        Some(fields) => {
            let projected_schema =
                ArrowReadContext::project_schema(full_arrow_schema, fields)?;
            let columns: Vec<_> = fields
                .iter()
                .map(|&idx| full_batch.column(idx).clone())
                .collect();
            Ok(RecordBatch::try_new(projected_schema, columns)?)
        }
    }
}
