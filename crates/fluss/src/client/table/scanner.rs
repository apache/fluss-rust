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

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use log::{debug, warn};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::client::connection::FlussConnection;
use crate::client::credentials::CredentialsCache;
use crate::client::metadata::Metadata;
use crate::client::table::log_fetch_buffer::{
    CompletedFetch, DefaultCompletedFetch, FetchErrorAction, FetchErrorContext, FetchErrorLogLevel,
    LogFetchBuffer,
};
use crate::client::table::remote_log::{
    RemoteLogDownloader, RemoteLogFetchInfo, RemotePendingFetch,
};
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TableInfo, TablePath};
use crate::proto::{ErrorResponse, FetchLogRequest, PbFetchLogReqForBucket, PbFetchLogReqForTable};
use crate::record::{LogRecordsBatches, ReadContext, ScanRecord, ScanRecords, to_arrow_schema};
use crate::rpc::{RpcClient, RpcError, message};
use crate::util::FairBucketStatusMap;

const LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
#[allow(dead_code)]
const LOG_FETCH_MAX_BYTES_FOR_BUCKET: i32 = 1024;
const LOG_FETCH_MIN_BYTES: i32 = 1;
const LOG_FETCH_WAIT_MAX_TIME: i32 = 500;

pub struct TableScan<'a> {
    conn: &'a FlussConnection,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    /// Column indices to project. None means all columns, Some(vec) means only the specified columns (non-empty).
    projected_fields: Option<Vec<usize>>,
}

impl<'a> TableScan<'a> {
    pub fn new(conn: &'a FlussConnection, table_info: TableInfo, metadata: Arc<Metadata>) -> Self {
        Self {
            conn,
            table_info,
            metadata,
            projected_fields: None,
        }
    }

    /// Projects the scan to only include specified columns by their indices.
    ///
    /// # Arguments
    /// * `column_indices` - Zero-based indices of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_indices` is empty or if any column index is out of range.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::InternalRow;
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_server = Some("127.0.0.1:9123".to_string());
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin().await?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table_info = admin.get_table(&table_path).await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by indices
    ///     let scanner = table.new_scan().project(&[0, 2, 3])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}, {}}}@{}",
    ///             row.get_int(0),
    ///             row.get_string(2),
    ///             row.get_string(3),
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project(mut self, column_indices: &[usize]) -> Result<Self> {
        if column_indices.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column indices cannot be empty".to_string(),
            });
        }
        let field_count = self.table_info.row_type().fields().len();
        for &idx in column_indices {
            if idx >= field_count {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Column index {} out of range (max: {})",
                        idx,
                        field_count - 1
                    ),
                });
            }
        }
        self.projected_fields = Some(column_indices.to_vec());
        Ok(self)
    }

    /// Projects the scan to only include specified columns by their names.
    ///
    /// # Arguments
    /// * `column_names` - Names of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_names` is empty or if any column name is not found in the table schema.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::InternalRow;
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_server = Some("127.0.0.1:9123".to_string());
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin().await?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table_info = admin.get_table(&table_path).await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by column names
    ///     let scanner = table.new_scan().project_by_name(&["col1", "col3"])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}}}@{}",
    ///             row.get_int(0),
    ///             row.get_string(1),
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project_by_name(mut self, column_names: &[&str]) -> Result<Self> {
        if column_names.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column names cannot be empty".to_string(),
            });
        }
        let row_type = self.table_info.row_type();
        let mut indices = Vec::new();

        for name in column_names {
            let idx = row_type
                .fields()
                .iter()
                .position(|f| f.name() == *name)
                .ok_or_else(|| Error::IllegalArgument {
                    message: format!("Column '{name}' not found"),
                })?;
            indices.push(idx);
        }

        self.projected_fields = Some(indices);
        Ok(self)
    }

    pub fn create_log_scanner(self) -> Result<LogScanner> {
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.projected_fields,
        )?;
        Ok(LogScanner {
            inner: Arc::new(inner),
        })
    }

    pub fn create_record_batch_log_scanner(self) -> Result<RecordBatchLogScanner> {
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.projected_fields,
        )?;
        Ok(RecordBatchLogScanner {
            inner: Arc::new(inner),
        })
    }
}

/// Scanner for reading log records one at a time with per-record metadata.
///
/// Use this scanner when you need access to individual record offsets and timestamps.
/// For batch-level access, use [`RecordBatchLogScanner`] instead.
pub struct LogScanner {
    inner: Arc<LogScannerInner>,
}

/// Scanner for reading log data as Arrow RecordBatches.
///
/// More efficient than [`LogScanner`] for batch-level analytics where per-record
/// metadata (offsets, timestamps) is not needed.
pub struct RecordBatchLogScanner {
    inner: Arc<LogScannerInner>,
}

/// Private shared implementation for both scanner types
struct LogScannerInner {
    table_path: TablePath,
    table_id: i64,
    metadata: Arc<Metadata>,
    log_scanner_status: Arc<LogScannerStatus>,
    log_fetcher: LogFetcher,
}

impl LogScannerInner {
    fn new(
        table_info: &TableInfo,
        metadata: Arc<Metadata>,
        connections: Arc<RpcClient>,
        projected_fields: Option<Vec<usize>>,
    ) -> Result<Self> {
        let log_scanner_status = Arc::new(LogScannerStatus::new());
        Ok(Self {
            table_path: table_info.table_path.clone(),
            table_id: table_info.table_id,
            metadata: metadata.clone(),
            log_scanner_status: log_scanner_status.clone(),
            log_fetcher: LogFetcher::new(
                table_info.clone(),
                connections.clone(),
                metadata.clone(),
                log_scanner_status.clone(),
                projected_fields,
            )?,
        })
    }

    async fn poll_records(&self, timeout: Duration) -> Result<ScanRecords> {
        let start = std::time::Instant::now();
        let deadline = start + timeout;

        loop {
            // Try to collect fetches
            let fetch_result = self.poll_for_fetches().await?;

            if !fetch_result.is_empty() {
                // We have data, send next round of fetches and return
                // This enables pipelining while user processes the data
                self.log_fetcher.send_fetches().await?;
                return Ok(ScanRecords::new(fetch_result));
            }

            // No data available, check if we should wait
            let now = std::time::Instant::now();
            if now >= deadline {
                // Timeout reached, return empty result
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Wait for buffer to become non-empty with remaining time
            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                // Timeout while waiting
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Buffer became non-empty, try again
        }
    }

    async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    async fn subscribe_batch(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        if bucket_offsets.is_empty() {
            return Err(Error::UnexpectedError {
                message: "Bucket offsets are empty.".to_string(),
                source: None,
            });
        }

        let mut scan_bucket_offsets = HashMap::new();
        for (bucket_id, offset) in bucket_offsets {
            let table_bucket = TableBucket::new(self.table_id, *bucket_id);
            scan_bucket_offsets.insert(table_bucket, *offset);
        }

        self.log_scanner_status
            .assign_scan_buckets(scan_bucket_offsets);
        Ok(())
    }

    async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let result = self.log_fetcher.collect_fetches()?;
        if !result.is_empty() {
            return Ok(result);
        }

        // send any new fetches (won't resend pending fetches).
        self.log_fetcher.send_fetches().await?;

        // Collect completed fetches from buffer
        self.log_fetcher.collect_fetches()
    }

    async fn poll_batches(&self, timeout: Duration) -> Result<Vec<RecordBatch>> {
        let start = std::time::Instant::now();
        let deadline = start + timeout;

        loop {
            let batches = self.poll_for_batches().await?;

            if !batches.is_empty() {
                self.log_fetcher.send_fetches().await?;
                return Ok(batches);
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }

            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                return Ok(Vec::new());
            }
        }
    }

    async fn poll_for_batches(&self) -> Result<Vec<RecordBatch>> {
        let result = self.log_fetcher.collect_batches()?;
        if !result.is_empty() {
            return Ok(result);
        }

        self.log_fetcher.send_fetches().await?;
        self.log_fetcher.collect_batches()
    }
}

// Implementation for LogScanner (records mode)
impl LogScanner {
    pub async fn poll(&self, timeout: Duration) -> Result<ScanRecords> {
        self.inner.poll_records(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_batch(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_batch(bucket_offsets).await
    }
}

// Implementation for RecordBatchLogScanner (batches mode)
impl RecordBatchLogScanner {
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<RecordBatch>> {
        self.inner.poll_batches(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_batch(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_batch(bucket_offsets).await
    }
}

struct LogFetcher {
    conns: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_path: TablePath,
    is_partitioned: bool,
    log_scanner_status: Arc<LogScannerStatus>,
    read_context: ReadContext,
    remote_read_context: ReadContext,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    // todo: consider schedule a background thread to update
    // token instead of update in fetch phase
    credentials_cache: Arc<CredentialsCache>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    nodes_with_pending_fetch_requests: Arc<Mutex<HashSet<i32>>>,
}

struct FetchResponseContext {
    metadata: Arc<Metadata>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    log_scanner_status: Arc<LogScannerStatus>,
    read_context: ReadContext,
    remote_read_context: ReadContext,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    credentials_cache: Arc<CredentialsCache>,
}

impl LogFetcher {
    pub fn new(
        table_info: TableInfo,
        conns: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        log_scanner_status: Arc<LogScannerStatus>,
        projected_fields: Option<Vec<usize>>,
    ) -> Result<Self> {
        let full_arrow_schema = to_arrow_schema(table_info.get_row_type())?;
        let read_context =
            Self::create_read_context(full_arrow_schema.clone(), projected_fields.clone(), false)?;
        let remote_read_context =
            Self::create_read_context(full_arrow_schema, projected_fields.clone(), true)?;

        let tmp_dir = TempDir::with_prefix("fluss-remote-logs")?;
        let log_fetch_buffer = Arc::new(LogFetchBuffer::new(read_context.clone()));

        Ok(LogFetcher {
            conns: conns.clone(),
            metadata: metadata.clone(),
            table_path: table_info.table_path.clone(),
            is_partitioned: table_info.is_partitioned(),
            log_scanner_status,
            read_context,
            remote_read_context,
            remote_log_downloader: Arc::new(RemoteLogDownloader::new(tmp_dir)?),
            credentials_cache: Arc::new(CredentialsCache::new(conns.clone(), metadata.clone())),
            log_fetch_buffer,
            nodes_with_pending_fetch_requests: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    fn create_read_context(
        full_arrow_schema: SchemaRef,
        projected_fields: Option<Vec<usize>>,
        is_from_remote: bool,
    ) -> Result<ReadContext> {
        match projected_fields {
            None => Ok(ReadContext::new(full_arrow_schema, is_from_remote)),
            Some(fields) => {
                ReadContext::with_projection_pushdown(full_arrow_schema, fields, is_from_remote)
            }
        }
    }

    fn describe_fetch_error(
        error: FlussError,
        table_bucket: &TableBucket,
        fetch_offset: i64,
        error_message: &str,
    ) -> FetchErrorContext {
        match error {
            FlussError::NotLeaderOrFollower
            | FlussError::LogStorageException
            | FlussError::KvStorageException
            | FlussError::StorageException
            | FlussError::FencedLeaderEpochException
            | FlussError::LeaderNotAvailableException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Error in fetch for bucket {table_bucket}: {error:?}: {error_message}"
                ),
            },
            FlussError::UnknownTableOrBucketException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Received unknown table or bucket error in fetch for bucket {table_bucket}"
                ),
            },
            FlussError::LogOffsetOutOfRangeException => FetchErrorContext {
                action: FetchErrorAction::LogOffsetOutOfRange,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "The fetching offset {fetch_offset} is out of range for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::AuthorizationException => FetchErrorContext {
                action: FetchErrorAction::Authorization,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Authorization error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::UnknownServerError => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Unknown server error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::CorruptMessage => FetchErrorContext {
                action: FetchErrorAction::CorruptMessage,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            _ => FetchErrorContext {
                action: FetchErrorAction::Unexpected,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                ),
            },
        }
    }

    fn should_invalidate_table_meta(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NotLeaderOrFollower
                | FlussError::LeaderNotAvailableException
                | FlussError::FencedLeaderEpochException
                | FlussError::UnknownTableOrBucketException
                | FlussError::InvalidCoordinatorException
        )
    }

    async fn check_and_update_metadata(&self) -> Result<()> {
        let need_update = self
            .fetchable_buckets()
            .iter()
            .any(|bucket| self.get_table_bucket_leader(bucket).is_none());

        if !need_update {
            return Ok(());
        }

        if self.is_partitioned {
            // Fallback to full table metadata refresh until partition-aware updates are available.
            self.metadata
                .update_tables_metadata(&HashSet::from([&self.table_path]))
                .await
                .or_else(|e| {
                    if let Error::RpcError { source, .. } = &e
                        && matches!(source, RpcError::ConnectionError(_) | RpcError::Poisoned(_))
                    {
                        warn!(
                            "Retrying after encountering error while updating table metadata: {e}"
                        );
                        Ok(())
                    } else {
                        Err(e)
                    }
                })?;
            return Ok(());
        }

        // TODO: Handle PartitionNotExist error
        self.metadata
            .update_tables_metadata(&HashSet::from([&self.table_path]))
            .await
            .or_else(|e| {
                if let Error::RpcError { source, .. } = &e
                    && matches!(source, RpcError::ConnectionError(_) | RpcError::Poisoned(_))
                {
                    warn!("Retrying after encountering error while updating table metadata: {e}");
                    Ok(())
                } else {
                    Err(e)
                }
            })
    }

    /// Send fetch requests asynchronously without waiting for responses
    async fn send_fetches(&self) -> Result<()> {
        self.check_and_update_metadata().await?;
        let fetch_request = self.prepare_fetch_log_requests().await;

        for (leader, fetch_request) in fetch_request {
            debug!("Adding pending request for node id {leader}");
            // Check if we already have a pending request for this node
            {
                self.nodes_with_pending_fetch_requests.lock().insert(leader);
            }

            let cluster = self.metadata.get_cluster().clone();

            let conns = Arc::clone(&self.conns);
            let log_fetch_buffer = self.log_fetch_buffer.clone();
            let log_scanner_status = self.log_scanner_status.clone();
            let read_context = self.read_context.clone();
            let remote_read_context = self.remote_read_context.clone();
            let remote_log_downloader = Arc::clone(&self.remote_log_downloader);
            let creds_cache = self.credentials_cache.clone();
            let nodes_with_pending = self.nodes_with_pending_fetch_requests.clone();
            let metadata = self.metadata.clone();
            let response_context = FetchResponseContext {
                metadata: metadata.clone(),
                log_fetch_buffer,
                log_scanner_status,
                read_context,
                remote_read_context,
                remote_log_downloader,
                credentials_cache: creds_cache,
            };
            // Spawn async task to handle the fetch request
            // Note: These tasks are not explicitly tracked or cancelled when LogFetcher is dropped.
            // This is acceptable because:
            // 1. Tasks will naturally complete (network requests will return or timeout)
            // 2. Tasks use Arc references, so resources are properly shared
            // 3. When the program exits, tokio runtime will clean up all tasks
            // 4. Tasks are short-lived (network I/O operations)
            tokio::spawn(async move {
                // make sure it will always remove leader from pending nodes
                let _guard = scopeguard::guard((), |_| {
                    nodes_with_pending.lock().remove(&leader);
                });

                let server_node = match cluster.get_tablet_server(leader) {
                    Some(node) => node,
                    None => {
                        warn!("No server node found for leader {leader}, retrying");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                let con = match conns.get_connection(server_node).await {
                    Ok(con) => con,
                    Err(e) => {
                        warn!("Retrying after error getting connection to destination node: {e:?}");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                let fetch_response = match con
                    .request(message::FetchLogRequest::new(fetch_request.clone()))
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!(
                            "Retrying after error fetching log from destination node {server_node:?}: {e:?}"
                        );
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                Self::handle_fetch_response(fetch_response, response_context).await;
            });
        }

        Ok(())
    }

    async fn handle_fetch_failure(
        metadata: Arc<Metadata>,
        server_id: &i32,
        request: &FetchLogRequest,
    ) {
        let table_ids = request.tables_req.iter().map(|r| r.table_id).collect();
        metadata.invalidate_server(server_id, table_ids);
    }

    /// Handle fetch response and add completed fetches to buffer
    async fn handle_fetch_response(
        fetch_response: crate::proto::FetchLogResponse,
        context: FetchResponseContext,
    ) {
        let FetchResponseContext {
            metadata,
            log_fetch_buffer,
            log_scanner_status,
            read_context,
            remote_read_context,
            remote_log_downloader,
            credentials_cache,
        } = context;

        for pb_fetch_log_resp in fetch_response.tables_resp {
            let table_id = pb_fetch_log_resp.table_id;
            let fetch_log_for_buckets = pb_fetch_log_resp.buckets_resp;

            for fetch_log_for_bucket in fetch_log_for_buckets {
                let bucket: i32 = fetch_log_for_bucket.bucket_id;
                let table_bucket = TableBucket::new(table_id, bucket);

                // todo: check fetch result code for per-bucket
                let Some(fetch_offset) = log_scanner_status.get_bucket_offset(&table_bucket) else {
                    debug!(
                        "Ignoring fetch log response for bucket {table_bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                };

                if let Some(error_code) = fetch_log_for_bucket.error_code
                    && error_code != FlussError::None.code()
                {
                    let api_error: ApiError = ErrorResponse {
                        error_code,
                        error_message: fetch_log_for_bucket.error_message.clone(),
                    }
                    .into();

                    let error = FlussError::for_code(error_code);
                    if Self::should_invalidate_table_meta(error) {
                        // TODO: Consider triggering table meta invalidation from sender/lookup paths.
                        let table_id = table_bucket.table_id();
                        let cluster = metadata.get_cluster();
                        if let Some(table_path) = cluster.get_table_path_by_id(table_id) {
                            let physical_tables =
                                HashSet::from([PhysicalTablePath::of(table_path.clone())]);
                            metadata.invalidate_physical_table_meta(&physical_tables);
                        } else {
                            warn!(
                                "Table id {table_id} is missing from table_path_by_id while invalidating table metadata"
                            );
                        }
                    }
                    let error_context = Self::describe_fetch_error(
                        error,
                        &table_bucket,
                        fetch_offset,
                        api_error.message.as_str(),
                    );
                    log_scanner_status.move_bucket_to_end(table_bucket.clone());
                    match error_context.log_level {
                        FetchErrorLogLevel::Debug => {
                            debug!("{}", error_context.log_message);
                        }
                        FetchErrorLogLevel::Warn => {
                            warn!("{}", error_context.log_message);
                        }
                    }
                    log_fetch_buffer.add_api_error(
                        table_bucket.clone(),
                        api_error,
                        error_context,
                        fetch_offset,
                    );
                    continue;
                }

                // Check if this is a remote log fetch
                if let Some(ref remote_log_fetch_info) = fetch_log_for_bucket.remote_log_fetch_info
                {
                    // set remote fs props
                    let remote_fs_props = credentials_cache.get_or_refresh().await.unwrap();
                    remote_log_downloader.set_remote_fs_props(remote_fs_props);

                    let remote_fetch_info =
                        RemoteLogFetchInfo::from_proto(remote_log_fetch_info, table_bucket.clone());

                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    Self::pending_remote_fetches(
                        remote_log_downloader.clone(),
                        log_fetch_buffer.clone(),
                        remote_read_context.clone(),
                        &table_bucket,
                        remote_fetch_info,
                        fetch_offset,
                        high_watermark,
                    );
                } else if fetch_log_for_bucket.records.is_some() {
                    // Handle regular in-memory records - create completed fetch directly
                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    let records = fetch_log_for_bucket.records.unwrap_or(vec![]);
                    let size_in_bytes = records.len();
                    let log_record_batch = LogRecordsBatches::new(records);

                    let completed_fetch = DefaultCompletedFetch::new(
                        table_bucket.clone(),
                        log_record_batch,
                        size_in_bytes,
                        read_context.clone(),
                        fetch_offset,
                        high_watermark,
                    );
                    log_fetch_buffer.add(Box::new(completed_fetch));
                }
            }
        }
    }

    fn pending_remote_fetches(
        remote_log_downloader: Arc<RemoteLogDownloader>,
        log_fetch_buffer: Arc<LogFetchBuffer>,
        read_context: ReadContext,
        table_bucket: &TableBucket,
        remote_fetch_info: RemoteLogFetchInfo,
        fetch_offset: i64,
        high_watermark: i64,
    ) {
        // Download and process remote log segments
        let mut pos_in_log_segment = remote_fetch_info.first_start_pos;
        let mut current_fetch_offset = fetch_offset;
        for (i, segment) in remote_fetch_info.remote_log_segments.iter().enumerate() {
            if i > 0 {
                pos_in_log_segment = 0;
                current_fetch_offset = segment.start_offset;
            }

            // todo:
            // 1: control the max threads to download remote segment
            // 2: introduce priority queue to priority highest for earliest segment
            let download_future = remote_log_downloader
                .request_remote_log(&remote_fetch_info.remote_log_tablet_dir, segment);

            // Register callback to be called when download completes
            // (similar to Java's downloadFuture.onComplete)
            // This must be done before creating RemotePendingFetch to avoid move issues
            let table_bucket = table_bucket.clone();
            let log_fetch_buffer_clone = log_fetch_buffer.clone();
            download_future.on_complete(move || {
                log_fetch_buffer_clone.try_complete(&table_bucket);
            });

            let pending_fetch = RemotePendingFetch::new(
                segment.clone(),
                download_future,
                pos_in_log_segment,
                current_fetch_offset,
                high_watermark,
                read_context.clone(),
            );
            // Add to pending fetches in buffer (similar to Java's logFetchBuffer.pend)
            log_fetch_buffer.pend(Box::new(pending_fetch));
        }
    }

    /// Collect completed fetches from buffer
    /// Reference: LogFetchCollector.collectFetch in Java
    fn collect_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        const MAX_POLL_RECORDS: usize = 500; // Default max poll records
        let mut result: HashMap<TableBucket, Vec<ScanRecord>> = HashMap::new();
        let mut records_remaining = MAX_POLL_RECORDS;

        let collect_result: Result<()> = {
            while records_remaining > 0 {
                // Get the next in line fetch, or get a new one from buffer
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                if next_in_line.is_none() || next_in_line.as_ref().unwrap().is_consumed() {
                    // Get a new fetch from buffer
                    if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                        // Initialize the fetch if not already initialized
                        if !completed_fetch.is_initialized() {
                            let size_in_bytes = completed_fetch.size_in_bytes();
                            match self.initialize_fetch(completed_fetch) {
                                Ok(initialized) => {
                                    self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                    continue;
                                }
                                Err(e) => {
                                    // Remove a completedFetch upon a parse with exception if
                                    // (1) it contains no records, and
                                    // (2) there are no fetched records with actual content preceding this
                                    // exception.
                                    if result.is_empty() && size_in_bytes == 0 {
                                        // todo: do we need to consider it like java ?
                                        // self.log_fetch_buffer.poll();
                                    }
                                    return Err(e);
                                }
                            }
                        } else {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(completed_fetch));
                        }
                        // Note: poll() already removed the fetch from buffer, so no need to call poll()
                    } else {
                        // No more fetches available
                        break;
                    }
                } else {
                    // Fetch records from next_in_line
                    if let Some(mut next_fetch) = next_in_line {
                        let records = match self
                            .fetch_records_from_fetch(&mut next_fetch, records_remaining)
                        {
                            Ok(records) => records,
                            Err(e) => {
                                if !next_fetch.is_consumed() {
                                    self.log_fetch_buffer
                                        .set_next_in_line_fetch(Some(next_fetch));
                                }
                                return Err(e);
                            }
                        };

                        if !records.is_empty() {
                            let table_bucket = next_fetch.table_bucket().clone();
                            // Merge with existing records for this bucket
                            let existing = result.entry(table_bucket).or_default();
                            let records_count = records.len();
                            existing.extend(records);

                            records_remaining = records_remaining.saturating_sub(records_count);
                        }

                        // If the fetch is not fully consumed, put it back for the next round
                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                        // If consumed, next_fetch will be dropped here (which is correct)
                    }
                }
            }
            Ok(())
        };

        match collect_result {
            Ok(()) => Ok(result),
            Err(e) => {
                if result.is_empty() {
                    Err(e)
                } else {
                    Ok(result)
                }
            }
        }
    }

    /// Initialize a completed fetch, checking offset match and updating high watermark
    fn initialize_fetch(
        &self,
        mut completed_fetch: Box<dyn CompletedFetch>,
    ) -> Result<Option<Box<dyn CompletedFetch>>> {
        if let Some(error) = completed_fetch.take_error() {
            return Err(error);
        }

        let table_bucket = completed_fetch.table_bucket().clone();
        let fetch_offset = completed_fetch.next_fetch_offset();

        if let Some(api_error) = completed_fetch.api_error() {
            let error = FlussError::for_code(api_error.code);
            let error_message = api_error.message.as_str();
            self.log_scanner_status
                .move_bucket_to_end(table_bucket.clone());
            let action = completed_fetch
                .fetch_error_context()
                .map(|context| context.action)
                .unwrap_or(FetchErrorAction::Unexpected);
            match action {
                FetchErrorAction::Ignore => {
                    return Ok(None);
                }
                FetchErrorAction::LogOffsetOutOfRange => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "The fetching offset {fetch_offset} is out of range: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Authorization => {
                    return Err(Error::FlussAPIError {
                        api_error: ApiError {
                            code: api_error.code,
                            message: api_error.message.to_string(),
                        },
                    });
                }
                FetchErrorAction::CorruptMessage => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Unexpected => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
            }
        }

        // Check if bucket is still subscribed
        let Some(current_offset) = self.log_scanner_status.get_bucket_offset(&table_bucket) else {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since the bucket has been unsubscribed"
            );
            return Ok(None);
        };

        // Check if offset matches
        if fetch_offset != current_offset {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since its offset {fetch_offset} does not match the expected offset {current_offset}"
            );
            return Ok(None);
        }

        // Update high watermark
        let high_watermark = completed_fetch.high_watermark();
        if high_watermark >= 0 {
            self.log_scanner_status
                .update_high_watermark(&table_bucket, high_watermark);
        }

        completed_fetch.set_initialized();
        Ok(Some(completed_fetch))
    }

    /// Fetch records from a completed fetch, checking offset match
    fn fetch_records_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_records: usize,
    ) -> Result<Vec<ScanRecord>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched records for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(Vec::new());
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        // Check if this fetch is next in line
        if fetch_offset == current_offset {
            let records = next_in_line_fetch.fetch_records(max_records)?;
            let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

            if next_fetch_offset > current_offset {
                self.log_scanner_status
                    .update_offset(&table_bucket, next_fetch_offset);
            }

            if next_in_line_fetch.is_consumed() && next_in_line_fetch.records_read() > 0 {
                self.log_scanner_status
                    .move_bucket_to_end(table_bucket.clone());
            }

            Ok(records)
        } else {
            // These records aren't next in line, ignore them
            warn!(
                "Ignoring fetched records for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(Vec::new())
        }
    }

    /// Collect completed fetches as RecordBatches
    fn collect_batches(&self) -> Result<Vec<RecordBatch>> {
        // Limit memory usage with both batch count and byte size constraints.
        // Max 100 batches per poll, but also check total bytes (soft cap ~64MB).
        const MAX_BATCHES: usize = 100;
        const MAX_BYTES: usize = 64 * 1024 * 1024; // 64MB soft cap
        let mut result: Vec<RecordBatch> = Vec::new();
        let mut batches_remaining = MAX_BATCHES;
        let mut bytes_consumed: usize = 0;

        let collect_result: Result<()> = {
            while batches_remaining > 0 && bytes_consumed < MAX_BYTES {
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                match next_in_line {
                    Some(mut next_fetch) if !next_fetch.is_consumed() => {
                        let batches =
                            self.fetch_batches_from_fetch(&mut next_fetch, batches_remaining)?;
                        let batch_count = batches.len();

                        if !batches.is_empty() {
                            // Track bytes consumed (soft cap - may exceed by one fetch)
                            let batch_bytes: usize =
                                batches.iter().map(|b| b.get_array_memory_size()).sum();
                            bytes_consumed += batch_bytes;

                            result.extend(batches);
                            batches_remaining = batches_remaining.saturating_sub(batch_count);
                        }

                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                    }
                    _ => {
                        if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                            if !completed_fetch.is_initialized() {
                                let size_in_bytes = completed_fetch.size_in_bytes();
                                match self.initialize_fetch(completed_fetch) {
                                    Ok(initialized) => {
                                        self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                        continue;
                                    }
                                    Err(e) => {
                                        if result.is_empty() && size_in_bytes == 0 {
                                            continue;
                                        }
                                        return Err(e);
                                    }
                                }
                            } else {
                                self.log_fetch_buffer
                                    .set_next_in_line_fetch(Some(completed_fetch));
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            Ok(())
        };

        match collect_result {
            Ok(()) => Ok(result),
            Err(e) => {
                if result.is_empty() {
                    Err(e)
                } else {
                    Ok(result)
                }
            }
        }
    }

    fn fetch_batches_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_batches: usize,
    ) -> Result<Vec<RecordBatch>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(Vec::new());
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        if fetch_offset == current_offset {
            let batches = next_in_line_fetch.fetch_batches(max_batches)?;
            let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

            if next_fetch_offset > current_offset {
                self.log_scanner_status
                    .update_offset(&table_bucket, next_fetch_offset);
            }

            Ok(batches)
        } else {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(Vec::new())
        }
    }

    async fn prepare_fetch_log_requests(&self) -> HashMap<i32, FetchLogRequest> {
        let mut fetch_log_req_for_buckets = HashMap::new();
        let mut table_id = None;
        let mut ready_for_fetch_count = 0;
        for bucket in self.fetchable_buckets() {
            if table_id.is_none() {
                table_id = Some(bucket.table_id());
            }

            let offset = match self.log_scanner_status.get_bucket_offset(&bucket) {
                Some(offset) => offset,
                None => {
                    debug!(
                        "Skipping fetch request for bucket {bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                }
            };

            match self.get_table_bucket_leader(&bucket) {
                None => {
                    log::trace!(
                        "Skipping fetch request for bucket {bucket} because leader is not available."
                    )
                }
                Some(leader) => {
                    if self
                        .nodes_with_pending_fetch_requests
                        .lock()
                        .contains(&leader)
                    {
                        log::trace!(
                            "Skipping fetch request for bucket {bucket} because previous request to server {leader} has not been processed."
                        )
                    } else {
                        let fetch_log_req_for_bucket = PbFetchLogReqForBucket {
                            partition_id: None,
                            bucket_id: bucket.bucket_id(),
                            fetch_offset: offset,
                            // 1M
                            max_fetch_bytes: 1024 * 1024,
                        };

                        fetch_log_req_for_buckets
                            .entry(leader)
                            .or_insert_with(Vec::new)
                            .push(fetch_log_req_for_bucket);
                        ready_for_fetch_count += 1;
                    }
                }
            }
        }

        if ready_for_fetch_count == 0 {
            HashMap::new()
        } else {
            let (projection_enabled, projected_fields) =
                match self.read_context.project_fields_in_order() {
                    None => (false, vec![]),
                    Some(fields) => (true, fields.iter().map(|&i| i as i32).collect()),
                };

            fetch_log_req_for_buckets
                .into_iter()
                .map(|(leader_id, feq_for_buckets)| {
                    let req_for_table = PbFetchLogReqForTable {
                        table_id: table_id.unwrap(),
                        projection_pushdown_enabled: projection_enabled,
                        projected_fields: projected_fields.clone(),
                        buckets_req: feq_for_buckets,
                    };

                    let fetch_log_request = FetchLogRequest {
                        follower_server_id: -1,
                        max_bytes: LOG_FETCH_MAX_BYTES,
                        tables_req: vec![req_for_table],
                        max_wait_ms: Some(LOG_FETCH_WAIT_MAX_TIME),
                        min_bytes: Some(LOG_FETCH_MIN_BYTES),
                    };
                    (leader_id, fetch_log_request)
                })
                .collect()
        }
    }

    fn fetchable_buckets(&self) -> Vec<TableBucket> {
        // Get buckets that are not already in the buffer
        let buffered = self.log_fetch_buffer.buffered_buckets();
        let buffered_set: HashSet<TableBucket> = buffered.into_iter().collect();
        self.log_scanner_status
            .fetchable_buckets(|tb| !buffered_set.contains(tb))
    }

    fn get_table_bucket_leader(&self, tb: &TableBucket) -> Option<i32> {
        let cluster = self.metadata.get_cluster();
        cluster.leader_for(tb).map(|leader| leader.id())
    }
}

pub struct LogScannerStatus {
    bucket_status_map: Arc<RwLock<FairBucketStatusMap<BucketScanStatus>>>,
}

#[allow(dead_code)]
impl LogScannerStatus {
    pub fn new() -> Self {
        Self {
            bucket_status_map: Arc::new(RwLock::new(FairBucketStatusMap::new())),
        }
    }

    pub fn prepare_to_poll(&self) -> bool {
        let map = self.bucket_status_map.read();
        map.size() > 0
    }

    pub fn move_bucket_to_end(&self, table_bucket: TableBucket) {
        let mut map = self.bucket_status_map.write();
        map.move_to_end(table_bucket);
    }

    /// Gets the offset of a bucket if it exists
    pub fn get_bucket_offset(&self, table_bucket: &TableBucket) -> Option<i64> {
        let map = self.bucket_status_map.read();
        map.status_value(table_bucket).map(|status| status.offset())
    }

    pub fn update_high_watermark(&self, table_bucket: &TableBucket, high_watermark: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_high_watermark(high_watermark);
        }
    }

    pub fn update_offset(&self, table_bucket: &TableBucket, offset: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_offset(offset);
        }
    }

    pub fn assign_scan_buckets(&self, scan_bucket_offsets: HashMap<TableBucket, i64>) {
        let mut map = self.bucket_status_map.write();
        for (bucket, offset) in scan_bucket_offsets {
            let status = map
                .status_value(&bucket)
                .cloned()
                .unwrap_or_else(|| Arc::new(BucketScanStatus::new(offset)));
            status.set_offset(offset);
            map.update(bucket, status);
        }
    }

    pub fn assign_scan_bucket(&self, table_bucket: TableBucket, offset: i64) {
        let status = Arc::new(BucketScanStatus::new(offset));
        self.bucket_status_map.write().update(table_bucket, status);
    }

    /// Unassigns scan buckets
    pub fn unassign_scan_buckets(&self, buckets: &[TableBucket]) {
        let mut map = self.bucket_status_map.write();
        for bucket in buckets {
            map.remove(bucket);
        }
    }

    /// Gets fetchable buckets based on availability predicate
    pub fn fetchable_buckets<F>(&self, is_available: F) -> Vec<TableBucket>
    where
        F: Fn(&TableBucket) -> bool,
    {
        let map = self.bucket_status_map.read();
        let mut result = Vec::new();
        map.for_each(|bucket, _| {
            if is_available(bucket) {
                result.push(bucket.clone());
            }
        });
        result
    }

    /// Helper to get bucket status
    fn get_status(&self, table_bucket: &TableBucket) -> Option<Arc<BucketScanStatus>> {
        let map = self.bucket_status_map.read();
        map.status_value(table_bucket).cloned()
    }
}

impl Default for LogScannerStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BucketScanStatus {
    offset: RwLock<i64>,
    high_watermark: RwLock<i64>,
}

#[allow(dead_code)]
impl BucketScanStatus {
    pub fn new(offset: i64) -> Self {
        Self {
            offset: RwLock::new(offset),
            high_watermark: RwLock::new(0),
        }
    }

    pub fn offset(&self) -> i64 {
        *self.offset.read()
    }

    pub fn set_offset(&self, offset: i64) {
        *self.offset.write() = offset
    }

    pub fn high_watermark(&self) -> i64 {
        *self.high_watermark.read()
    }

    pub fn set_high_watermark(&self, high_watermark: i64) {
        *self.high_watermark.write() = high_watermark
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::FlussConnection;
    use crate::client::WriteRecord;
    use crate::client::metadata::Metadata;
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::config::Config;
    use crate::metadata::{TableInfo, TablePath};
    use crate::proto::{FetchLogResponse, PbFetchLogRespForBucket, PbFetchLogRespForTable};
    use crate::record::MemoryLogRecordsArrowBuilder;
    use crate::row::{Datum, GenericRow};
    use crate::rpc::FlussError;
    use crate::test_utils::TestCompletedFetch;
    use crate::test_utils::{build_cluster_arc, build_mock_connection, build_table_info};
    use prost::Message;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::io::BufStream;
    use tokio::time::timeout;

    const DEFAULT_TABLE_ID: i64 = 1;
    const DEFAULT_BUCKETS: i32 = 1;

    fn default_table_path() -> TablePath {
        TablePath::new("db".to_string(), "tbl".to_string())
    }

    struct ScannerTestEnv {
        table_path: TablePath,
        table_info: TableInfo,
        metadata: Arc<Metadata>,
        status: Arc<LogScannerStatus>,
        rpc_client: Arc<RpcClient>,
    }

    impl ScannerTestEnv {
        fn new() -> Self {
            let table_path = default_table_path();
            let table_info =
                build_table_info(table_path.clone(), DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
            let cluster = build_cluster_arc(&table_path, DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
            Self::with_table_info_and_cluster(table_info, cluster)
        }

        fn with_table_info_and_cluster(table_info: TableInfo, cluster: Arc<Cluster>) -> Self {
            let metadata = Arc::new(Metadata::new_for_test(cluster));
            let status = Arc::new(LogScannerStatus::new());
            let rpc_client = Arc::new(RpcClient::new());
            let table_path = table_info.table_path.clone();
            Self {
                table_path,
                table_info,
                metadata,
                status,
                rpc_client,
            }
        }

        fn fetcher(&self, projected_fields: Option<Vec<usize>>) -> Result<LogFetcher> {
            LogFetcher::new(
                self.table_info.clone(),
                self.rpc_client.clone(),
                self.metadata.clone(),
                self.status.clone(),
                projected_fields,
            )
        }

        fn inner(&self, projected_fields: Option<Vec<usize>>) -> Result<LogScannerInner> {
            LogScannerInner::new(
                &self.table_info,
                self.metadata.clone(),
                self.rpc_client.clone(),
                projected_fields,
            )
        }

        fn connection(&self) -> FlussConnection {
            FlussConnection::new_for_test(
                self.metadata.clone(),
                self.rpc_client.clone(),
                Config::default(),
            )
        }

        fn assign_bucket(&self, bucket_id: i32, offset: i64) -> TableBucket {
            let bucket = TableBucket::new(self.table_info.table_id, bucket_id);
            self.status.assign_scan_bucket(bucket.clone(), offset);
            bucket
        }

        fn build_records(&self) -> Result<Vec<u8>> {
            build_records(&self.table_info, &self.table_path)
        }
    }

    fn build_records(table_info: &TableInfo, table_path: &TablePath) -> Result<Vec<u8>> {
        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            table_info.get_row_type(),
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
        )?;
        let record = WriteRecord::for_append(
            Arc::new(table_path.clone()),
            table_info.schema_id,
            GenericRow {
                values: vec![Datum::Int32(1)],
            },
        );
        builder.append(&record)?;
        builder.build()
    }

    fn build_cluster_with_leader(
        table_info: &TableInfo,
        leader: Option<ServerNode>,
        include_server: bool,
    ) -> Arc<Cluster> {
        let table_bucket = TableBucket::new(table_info.table_id, 0);
        let mut servers = HashMap::new();
        if include_server {
            if let Some(server) = leader.clone() {
                servers.insert(server.id(), server);
            }
        }
        let location =
            BucketLocation::new(table_bucket.clone(), leader, table_info.table_path.clone());
        let locations_by_path =
            HashMap::from([(table_info.table_path.clone(), vec![location.clone()])]);
        let locations_by_bucket = HashMap::from([(table_bucket, location)]);
        let table_id_by_path =
            HashMap::from([(table_info.table_path.clone(), table_info.table_id)]);
        let table_info_by_path =
            HashMap::from([(table_info.table_path.clone(), table_info.clone())]);
        Arc::new(Cluster::new(
            None,
            servers,
            locations_by_path,
            locations_by_bucket,
            table_id_by_path,
            table_info_by_path,
        ))
    }

    async fn collect_result_for_error(
        error: FlussError,
    ) -> Result<std::result::Result<HashMap<TableBucket, Vec<ScanRecord>>, Error>> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        let response = FetchLogResponse {
            tables_resp: vec![PbFetchLogRespForTable {
                table_id: env.table_info.table_id,
                buckets_resp: vec![PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(error.code()),
                    error_message: Some("err".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: env.metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            credentials_cache: fetcher.credentials_cache.clone(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        Ok(fetcher.collect_fetches())
    }

    #[test]
    fn project_rejects_empty_indices() -> Result<()> {
        let env = ScannerTestEnv::new();
        let conn = env.connection();

        let builder = TableScan::new(&conn, env.table_info.clone(), env.metadata.clone());
        let result = builder.project(&[]);
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    #[test]
    fn project_rejects_out_of_range_index() -> Result<()> {
        let env = ScannerTestEnv::new();
        let conn = env.connection();

        let builder = TableScan::new(&conn, env.table_info.clone(), env.metadata.clone());
        let result = builder.project(&[1]);
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    #[test]
    fn project_by_name_rejects_empty() -> Result<()> {
        let env = ScannerTestEnv::new();
        let conn = env.connection();

        let builder = TableScan::new(&conn, env.table_info.clone(), env.metadata.clone());
        let result = builder.project_by_name(&[]);
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    #[test]
    fn project_by_name_rejects_missing_column() -> Result<()> {
        let env = ScannerTestEnv::new();
        let conn = env.connection();

        let builder = TableScan::new(&conn, env.table_info.clone(), env.metadata.clone());
        let result = builder.project_by_name(&["missing"]);
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    #[test]
    fn log_fetcher_rejects_invalid_projection() -> Result<()> {
        let env = ScannerTestEnv::new();

        let result = env.fetcher(Some(vec![1]));
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    async fn wait_for_leader_removal(
        metadata: &Metadata,
        table_bucket: &TableBucket,
    ) -> Result<()> {
        timeout(Duration::from_millis(500), async {
            loop {
                if metadata.get_cluster().leader_for(table_bucket).is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| Error::UnexpectedError {
            message: "Timeout waiting for leader removal".to_string(),
            source: None,
        })?;
        Ok(())
    }

    #[tokio::test]
    async fn collect_fetches_updates_offset() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);

        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let read_context = ReadContext::new(to_arrow_schema(env.table_info.get_row_type())?, false);
        let completed =
            DefaultCompletedFetch::new(bucket.clone(), log_records, data.len(), read_context, 0, 0);
        fetcher.log_fetch_buffer.add(Box::new(completed));

        let fetched = fetcher.collect_fetches()?;
        assert_eq!(fetched.get(&bucket).unwrap().len(), 1);
        assert_eq!(env.status.get_bucket_offset(&bucket), Some(1));
        Ok(())
    }

    #[test]
    fn fetch_records_from_fetch_drains_unassigned_bucket() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = TableBucket::new(env.table_info.table_id, 0);
        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let read_context = ReadContext::new(to_arrow_schema(env.table_info.get_row_type())?, false);
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            bucket,
            log_records,
            data.len(),
            read_context,
            0,
            0,
        ));

        let records = fetcher.fetch_records_from_fetch(&mut completed, 10)?;
        assert!(records.is_empty());
        assert!(completed.is_consumed());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_fetch_log_requests_skips_pending() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        fetcher.nodes_with_pending_fetch_requests.lock().insert(1);

        let requests = fetcher.prepare_fetch_log_requests().await;
        assert!(requests.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_fetch_log_requests_skips_without_leader() -> Result<()> {
        let table_path = default_table_path();
        let table_info = build_table_info(table_path.clone(), DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
        let cluster = build_cluster_with_leader(&table_info, None, false);
        let env = ScannerTestEnv::with_table_info_and_cluster(table_info, cluster);
        env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        let requests = fetcher.prepare_fetch_log_requests().await;
        assert!(requests.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_fetch_log_requests_sets_projection() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 0);
        let fetcher = env.fetcher(Some(vec![0]))?;

        let requests = fetcher.prepare_fetch_log_requests().await;
        let request = requests.get(&1).expect("fetch request");
        let table_req = request.tables_req.first().expect("table request");
        assert!(table_req.projection_pushdown_enabled);
        assert_eq!(table_req.projected_fields, vec![0]);
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_sets_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 5);
        let fetcher = env.fetcher(None)?;

        let response = crate::proto::FetchLogResponse {
            tables_resp: vec![crate::proto::PbFetchLogRespForTable {
                table_id: env.table_info.table_id,
                buckets_resp: vec![crate::proto::PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(FlussError::AuthorizationException.code()),
                    error_message: Some("denied".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: env.metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            credentials_cache: fetcher.credentials_cache.clone(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        let completed = fetcher.log_fetch_buffer.poll().expect("completed fetch");
        let api_error = completed.api_error().expect("api error");
        assert_eq!(api_error.code, FlussError::AuthorizationException.code());
        Ok(())
    }

    #[tokio::test]
    async fn collect_fetches_ignores_retriable_errors() -> Result<()> {
        let ignore_errors = [
            FlussError::NotLeaderOrFollower,
            FlussError::LogStorageException,
            FlussError::KvStorageException,
            FlussError::StorageException,
            FlussError::FencedLeaderEpochException,
            FlussError::UnknownTableOrBucketException,
            FlussError::UnknownServerError,
        ];

        for error in ignore_errors {
            let result = collect_result_for_error(error).await?;
            assert!(
                matches!(result, Ok(records) if records.is_empty()),
                "unexpected result for {error:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn collect_fetches_returns_error_for_corrupt_or_unexpected() -> Result<()> {
        let error_cases = [
            FlussError::CorruptMessage,
            FlussError::InvalidTableException,
        ];

        for error in error_cases {
            let result = collect_result_for_error(error).await?;
            assert!(
                matches!(result, Err(Error::UnexpectedError { .. })),
                "unexpected result for {error:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn send_fetches_invalidates_missing_server() -> Result<()> {
        let table_path = default_table_path();
        let table_info = build_table_info(table_path.clone(), DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
        let leader = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let cluster = build_cluster_with_leader(&table_info, Some(leader), false);
        let env = ScannerTestEnv::with_table_info_and_cluster(table_info, cluster);
        let bucket = env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        fetcher.send_fetches().await?;
        wait_for_leader_removal(&env.metadata, &bucket).await?;
        Ok(())
    }

    #[tokio::test]
    async fn send_fetches_invalidates_on_request_error() -> Result<()> {
        let table_path = default_table_path();
        let table_info = build_table_info(table_path.clone(), DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
        let leader = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);
        let cluster = build_cluster_with_leader(&table_info, Some(leader.clone()), true);
        let env = ScannerTestEnv::with_table_info_and_cluster(table_info, cluster);
        let bucket = env.assign_bucket(0, 0);
        let rpc_client = env.rpc_client.clone();

        let (client, server) = tokio::io::duplex(1024);
        drop(server);
        let transport = crate::rpc::Transport::Test { inner: client };
        let connection = Arc::new(crate::rpc::ServerConnectionInner::new(
            BufStream::new(transport),
            usize::MAX,
            Arc::from(""),
        ));
        rpc_client.insert_connection_for_test(&leader, connection);

        let fetcher = env.fetcher(None)?;
        fetcher.send_fetches().await?;
        wait_for_leader_removal(&env.metadata, &bucket).await?;
        Ok(())
    }

    #[tokio::test]
    async fn send_fetches_invalidates_on_connection_error() -> Result<()> {
        let table_path = default_table_path();
        let table_info = build_table_info(table_path.clone(), DEFAULT_TABLE_ID, DEFAULT_BUCKETS);
        let leader = ServerNode::new(1, "127.0.0.1".to_string(), 1, ServerType::TabletServer);
        let cluster = build_cluster_with_leader(&table_info, Some(leader.clone()), true);
        let env = ScannerTestEnv::with_table_info_and_cluster(table_info, cluster);
        let bucket = env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        fetcher.send_fetches().await?;
        wait_for_leader_removal(&env.metadata, &bucket).await?;
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_records_are_collected() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        let records = env.build_records()?;
        let response = FetchLogResponse {
            tables_resp: vec![PbFetchLogRespForTable {
                table_id: env.table_info.table_id,
                buckets_resp: vec![PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: None,
                    error_message: None,
                    high_watermark: Some(5),
                    log_start_offset: Some(0),
                    remote_log_fetch_info: None,
                    records: Some(records),
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: env.metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            credentials_cache: fetcher.credentials_cache.clone(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        let fetched = fetcher.collect_fetches()?;
        assert_eq!(fetched.get(&TableBucket::new(1, 0)).unwrap().len(), 1);
        assert_eq!(
            env.status.get_bucket_offset(&TableBucket::new(1, 0)),
            Some(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn send_fetches_enqueues_completed_fetch() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 0);
        let rpc_client = env.rpc_client.clone();

        let records = env.build_records()?;
        let response = FetchLogResponse {
            tables_resp: vec![PbFetchLogRespForTable {
                table_id: env.table_info.table_id,
                buckets_resp: vec![PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: None,
                    error_message: None,
                    high_watermark: Some(1),
                    log_start_offset: Some(0),
                    remote_log_fetch_info: None,
                    records: Some(records.clone()),
                }],
            }],
        };

        let (connection, handle) =
            build_mock_connection(move |_api_key: crate::rpc::ApiKey, _, _| {
                response.encode_to_vec()
            })
            .await;
        let server_node = env
            .metadata
            .get_cluster()
            .get_tablet_server(1)
            .expect("server")
            .clone();
        rpc_client.insert_connection_for_test(&server_node, connection);

        let fetcher = env.fetcher(None)?;

        fetcher.send_fetches().await?;
        let has_data = fetcher
            .log_fetch_buffer
            .await_not_empty(Duration::from_millis(200))
            .await?;
        assert!(has_data);

        let fetched = fetcher.collect_fetches()?;
        assert_eq!(fetched.get(&TableBucket::new(1, 0)).unwrap().len(), 1);
        handle.abort();
        Ok(())
    }

    #[test]
    fn collect_batches_returns_batches_and_updates_offset() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);

        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let mut completed = DefaultCompletedFetch::new(
            bucket.clone(),
            log_records,
            data.len(),
            fetcher.read_context.clone(),
            0,
            0,
        );
        completed.set_initialized();
        fetcher
            .log_fetch_buffer
            .set_next_in_line_fetch(Some(Box::new(completed)));

        let batches = fetcher.collect_batches()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(env.status.get_bucket_offset(&bucket), Some(1));
        Ok(())
    }

    #[test]
    fn collect_batches_returns_partial_on_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);

        let completed = TestCompletedFetch::batch_ok(bucket.clone());
        fetcher
            .log_fetch_buffer
            .set_next_in_line_fetch(Some(Box::new(completed)));

        let error_fetch = TestCompletedFetch::batch_err(bucket.clone());
        fetcher.log_fetch_buffer.add(Box::new(error_fetch));

        let batches = fetcher.collect_batches()?;
        assert_eq!(batches.len(), 1);
        Ok(())
    }

    #[test]
    fn fetch_batches_from_fetch_drains_unassigned_bucket() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = TableBucket::new(env.table_info.table_id, 0);
        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            bucket,
            log_records,
            data.len(),
            fetcher.read_context.clone(),
            0,
            0,
        ));

        let batches = fetcher.fetch_batches_from_fetch(&mut completed, 10)?;
        assert!(batches.is_empty());
        assert!(completed.is_consumed());
        Ok(())
    }

    #[test]
    fn fetch_batches_from_fetch_returns_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::from_error(
            bucket,
            Error::UnexpectedError {
                message: "fetch error".to_string(),
                source: None,
            },
            0,
            fetcher.read_context.clone(),
        ));

        let result = fetcher.fetch_batches_from_fetch(&mut completed, 10);
        assert!(matches!(result, Err(Error::UnexpectedError { .. })));
        Ok(())
    }

    #[test]
    fn fetch_batches_from_fetch_ignores_out_of_order_offset() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 5);
        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            bucket,
            log_records,
            data.len(),
            fetcher.read_context.clone(),
            0,
            0,
        ));

        let batches = fetcher.fetch_batches_from_fetch(&mut completed, 10)?;
        assert!(batches.is_empty());
        assert!(completed.is_consumed());
        Ok(())
    }

    #[test]
    fn collect_batches_skips_error_when_empty_and_size_zero() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let error_fetch = DefaultCompletedFetch::from_error(
            TableBucket::new(1, 0),
            Error::UnexpectedError {
                message: "fetch error".to_string(),
                source: None,
            },
            0,
            fetcher.read_context.clone(),
        );
        fetcher.log_fetch_buffer.add(Box::new(error_fetch));

        let batches = fetcher.collect_batches()?;
        assert!(batches.is_empty());
        Ok(())
    }

    #[test]
    fn fetch_records_from_fetch_ignores_out_of_order_offset() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 5);
        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            bucket,
            log_records,
            data.len(),
            fetcher.read_context.clone(),
            0,
            0,
        ));

        let records = fetcher.fetch_records_from_fetch(&mut completed, 10)?;
        assert!(records.is_empty());
        assert!(completed.is_consumed());
        Ok(())
    }

    #[test]
    fn fetch_records_from_fetch_returns_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::from_error(
            bucket,
            Error::UnexpectedError {
                message: "fetch error".to_string(),
                source: None,
            },
            0,
            fetcher.read_context.clone(),
        ));

        let result = fetcher.fetch_records_from_fetch(&mut completed, 10);
        assert!(matches!(result, Err(Error::UnexpectedError { .. })));
        Ok(())
    }

    #[test]
    fn collect_fetches_returns_partial_on_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let fetcher = env.fetcher(None)?;

        let bucket = env.assign_bucket(0, 0);
        let completed = TestCompletedFetch::record_ok(bucket.clone());
        fetcher
            .log_fetch_buffer
            .set_next_in_line_fetch(Some(Box::new(completed)));

        let error_fetch = TestCompletedFetch::record_err(bucket.clone());
        fetcher.log_fetch_buffer.add(Box::new(error_fetch));

        let result = fetcher.collect_fetches()?;
        let records = result.get(&bucket).expect("records");
        assert_eq!(records.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_batch_rejects_empty() -> Result<()> {
        let env = ScannerTestEnv::new();
        let inner = env.inner(None)?;

        let result = inner.subscribe_batch(&HashMap::new()).await;
        assert!(matches!(result, Err(Error::UnexpectedError { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_invalidates_table_meta() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let status = Arc::new(LogScannerStatus::new());
        status.assign_scan_bucket(TableBucket::new(1, 0), 5);
        let fetcher = LogFetcher::new(
            table_info.clone(),
            Arc::new(RpcClient::new()),
            metadata.clone(),
            status.clone(),
            None,
        )?;

        let bucket = TableBucket::new(1, 0);
        assert!(metadata.leader_for(&bucket).is_some());

        let response = crate::proto::FetchLogResponse {
            tables_resp: vec![crate::proto::PbFetchLogRespForTable {
                table_id: 1,
                buckets_resp: vec![crate::proto::PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(FlussError::NotLeaderOrFollower.code()),
                    error_message: Some("not leader".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            credentials_cache: fetcher.credentials_cache.clone(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        assert!(metadata.leader_for(&bucket).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_out_of_range_sets_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 5);
        let fetcher = env.fetcher(None)?;

        let response = crate::proto::FetchLogResponse {
            tables_resp: vec![crate::proto::PbFetchLogRespForTable {
                table_id: env.table_info.table_id,
                buckets_resp: vec![crate::proto::PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(FlussError::LogOffsetOutOfRangeException.code()),
                    error_message: Some("out of range".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: env.metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            credentials_cache: fetcher.credentials_cache.clone(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        let completed = fetcher.log_fetch_buffer.poll().expect("completed fetch");
        let result = fetcher.initialize_fetch(completed);
        assert!(matches!(result, Err(Error::UnexpectedError { .. })));
        Ok(())
    }

    #[test]
    fn initialize_fetch_returns_authorization_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let bucket = env.assign_bucket(0, 0);
        let fetcher = env.fetcher(None)?;

        let error_context = LogFetcher::describe_fetch_error(
            FlussError::AuthorizationException,
            &bucket,
            0,
            "denied",
        );
        let completed = DefaultCompletedFetch::from_api_error(
            bucket,
            ApiError {
                code: FlussError::AuthorizationException.code(),
                message: "denied".to_string(),
            },
            error_context,
            0,
            fetcher.read_context.clone(),
        );

        let result = fetcher.initialize_fetch(Box::new(completed));
        assert!(matches!(result, Err(Error::FlussAPIError { .. })));
        Ok(())
    }

    #[test]
    fn initialize_fetch_discards_stale_offset() -> Result<()> {
        let env = ScannerTestEnv::new();
        env.assign_bucket(0, 5);
        let fetcher = env.fetcher(None)?;

        let data = env.build_records()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let read_context = ReadContext::new(to_arrow_schema(env.table_info.get_row_type())?, false);
        let completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            TableBucket::new(1, 0),
            log_records,
            data.len(),
            read_context,
            0,
            0,
        ));

        let result = fetcher.initialize_fetch(completed)?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn poll_without_subscription_returns_empty() -> Result<()> {
        let env = ScannerTestEnv::new();
        let inner = env.inner(None)?;
        let scanner = LogScanner {
            inner: Arc::new(inner),
        };

        let result = scanner.poll(Duration::from_millis(1)).await?;
        assert!(result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn poll_records_propagates_wakeup_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let inner = env.inner(None)?;

        inner.log_fetcher.log_fetch_buffer.wakeup();
        let result = inner.poll_records(Duration::from_millis(10)).await;
        assert!(matches!(result, Err(Error::WakeupError { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn poll_batches_propagates_wakeup_error() -> Result<()> {
        let env = ScannerTestEnv::new();
        let inner = env.inner(None)?;

        inner.log_fetcher.log_fetch_buffer.wakeup();
        let result = inner.poll_batches(Duration::from_millis(10)).await;
        assert!(matches!(result, Err(Error::WakeupError { .. })));
        Ok(())
    }
}
