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

use crate::client::ClientSchemaGetter;
use crate::client::metadata::Metadata;
use crate::client::table::batch_scanner::decode_kv_batch;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{TableBucket, TableInfo};
use crate::proto::{self, ErrorResponse};
use crate::record::ScanBatch;
use crate::rpc::RpcClient;
use crate::rpc::message::ScanKvRequest;
use log::warn;
use std::sync::Arc;

/// Maximum retry attempts for retriable server errors (e.g. leader election
/// races on a freshly created bucket, transient `TooManyScanners`).
const MAX_RETRIABLE_RETRIES: u32 = 5;

/// Stateful scanner for full KV table scans using the ScanKv API (1061).
///
/// The server maintains a cursor: the first `next_batch()` opens the scanner,
/// subsequent calls iterate, and dropping the scanner sends a best-effort close.
pub struct KvBatchScanner {
    bucket: TableBucket,
    state: ScannerState,
}

enum ScannerState {
    Pending(ScanContext),
    Active {
        ctx: ScanContext,
        scanner_id: Vec<u8>,
        call_seq_id: i32,
    },
    Done,
}

struct ScanContext {
    rpc_client: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    schema_getter: Arc<ClientSchemaGetter>,
    projected_fields: Option<Vec<usize>>,
    batch_size_bytes: i32,
}

impl KvBatchScanner {
    pub(super) fn new(
        rpc_client: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        table_info: TableInfo,
        schema_getter: Arc<ClientSchemaGetter>,
        projected_fields: Option<Vec<usize>>,
        bucket: TableBucket,
        batch_size_bytes: i32,
    ) -> Self {
        Self {
            bucket,
            state: ScannerState::Pending(ScanContext {
                rpc_client,
                metadata,
                table_info,
                schema_getter,
                projected_fields,
                batch_size_bytes,
            }),
        }
    }

    pub async fn next_batch(&mut self) -> Result<Option<ScanBatch>> {
        match std::mem::replace(&mut self.state, ScannerState::Done) {
            ScannerState::Done => Ok(None),
            ScannerState::Pending(ctx) => self.open_scanner(ctx).await,
            ScannerState::Active {
                ctx,
                scanner_id,
                call_seq_id,
            } => self.iterate(ctx, scanner_id, call_seq_id).await,
        }
    }

    pub async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }

    async fn open_scanner(&mut self, ctx: ScanContext) -> Result<Option<ScanBatch>> {
        let bucket_scan_req = proto::PbScanReqForBucket {
            table_id: ctx.table_info.table_id,
            partition_id: self.bucket.partition_id(),
            bucket_id: self.bucket.bucket_id(),
            limit: None,
        };
        let request = ScanKvRequest::new(
            None,
            Some(bucket_scan_req),
            Some(0),
            Some(ctx.batch_size_bytes),
            Some(false),
        );

        let response = self
            .send_with_retry(&ctx, request, MAX_RETRIABLE_RETRIES)
            .await?;

        self.handle_response(ctx, response, 0).await
    }

    async fn iterate(
        &mut self,
        ctx: ScanContext,
        scanner_id: Vec<u8>,
        call_seq_id: i32,
    ) -> Result<Option<ScanBatch>> {
        let next_seq = call_seq_id + 1;
        let request = ScanKvRequest::new(
            Some(scanner_id.clone()),
            None,
            Some(next_seq),
            Some(ctx.batch_size_bytes),
            Some(false),
        );

        let response = self
            .send_with_retry(&ctx, request, MAX_RETRIABLE_RETRIES)
            .await?;

        self.handle_response(ctx, response, next_seq).await
    }

    async fn handle_response(
        &mut self,
        ctx: ScanContext,
        response: proto::ScanKvResponse,
        call_seq_id: i32,
    ) -> Result<Option<ScanBatch>> {
        let scanner_id = response.scanner_id.unwrap_or_default();
        let has_more = response.has_more_results.unwrap_or(false);
        let raw = response.records.unwrap_or_default().to_vec();
        let log_offset = response.log_offset.unwrap_or(0);

        let batch = decode_kv_batch(
            &ctx.table_info,
            &ctx.schema_getter,
            ctx.projected_fields.as_deref(),
            raw,
            usize::MAX,
        )
        .await?;

        if has_more {
            self.state = ScannerState::Active {
                ctx,
                scanner_id,
                call_seq_id,
            };
        } else {
            self.state = ScannerState::Done;
        }

        if batch.num_rows() == 0 && !has_more {
            return Ok(None);
        }

        Ok(Some(ScanBatch::new(self.bucket.clone(), batch, log_offset)))
    }

    async fn send_with_retry(
        &self,
        ctx: &ScanContext,
        request: ScanKvRequest,
        max_retries: u32,
    ) -> Result<proto::ScanKvResponse> {
        let mut attempts = 0;
        loop {
            let leader = ctx
                .metadata
                .leader_for(&ctx.table_info.table_path, &self.bucket)
                .await?
                .ok_or_else(|| {
                    Error::leader_not_available(format!(
                        "No leader found for table bucket: {}",
                        self.bucket
                    ))
                })?;
            let connection = ctx.rpc_client.get_connection(&leader).await?;

            let req = rebuild_request(&request);
            let response = connection.request(req).await?;

            if let Some(error_code) = response.error_code
                && error_code != FlussError::None.code()
            {
                let fluss_error = FlussError::for_code(error_code);
                if fluss_error.is_retriable() && attempts < max_retries {
                    attempts += 1;
                    let delay_ms = 100u64 * (1u64 << attempts.min(5));
                    warn!(
                        "Retriable error {:?} (code {}) for bucket {}, retry {}/{} after {}ms",
                        fluss_error, error_code, self.bucket, attempts, max_retries, delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                let err: ApiError = ErrorResponse {
                    error_code,
                    error_message: response.error_message.clone(),
                }
                .into();
                return Err(Error::FlussAPIError { api_error: err });
            }

            return Ok(response);
        }
    }
}

fn rebuild_request(original: &ScanKvRequest) -> ScanKvRequest {
    ScanKvRequest::new(
        original.inner_request.scanner_id.clone(),
        original.inner_request.bucket_scan_req,
        original.inner_request.call_seq_id,
        original.inner_request.batch_size_bytes,
        original.inner_request.close_scanner,
    )
}

impl Drop for KvBatchScanner {
    fn drop(&mut self) {
        if let ScannerState::Active {
            ref ctx,
            ref scanner_id,
            call_seq_id,
        } = self.state
        {
            let rpc_client = ctx.rpc_client.clone();
            let metadata = ctx.metadata.clone();
            let table_path = ctx.table_info.table_path.clone();
            let bucket = self.bucket.clone();
            let scanner_id = scanner_id.clone();
            let close_seq = call_seq_id + 1;

            tokio::spawn(async move {
                let leader = match metadata.leader_for(&table_path, &bucket).await {
                    Ok(Some(leader)) => leader,
                    _ => return,
                };
                let connection = match rpc_client.get_connection(&leader).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let request =
                    ScanKvRequest::new(Some(scanner_id), None, Some(close_seq), None, Some(true));
                let _ = connection.request(request).await;
            });
        }
    }
}
