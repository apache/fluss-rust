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
use crate::client::credentials::CredentialsCache;
use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TableInfo, TablePath};
use crate::proto::{FetchLogRequest, PbFetchLogReqForBucket, PbFetchLogReqForTable};
use crate::record::{LogRecordsBatchs, ReadContext, ScanRecord, ScanRecords, to_arrow_schema};
use crate::rpc::RpcClient;
use crate::util::FairBucketStatusMap;
use arrow_schema::SchemaRef;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::slice::from_ref;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::client::table::remote_log::{
    RemoteLogDownloader, RemoteLogFetchInfo, RemotePendingFetch,
};

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
    /// let scanner = table.new_scan().project(&[0, 2, 3])?.create_log_scanner();
    /// ```
    pub fn project(mut self, column_indices: &[usize]) -> Result<Self> {
        if column_indices.is_empty() {
            return Err(Error::IllegalArgument(
                "Column indices cannot be empty".to_string(),
            ));
        }
        let field_count = self.table_info.row_type().fields().len();
        for &idx in column_indices {
            if idx >= field_count {
                return Err(Error::IllegalArgument(format!(
                    "Column index {} out of range (max: {})",
                    idx,
                    field_count - 1
                )));
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
    /// let scanner = table.new_scan().project_by_name(&["col1", "col3"])?.create_log_scanner();
    /// ```
    pub fn project_by_name(mut self, column_names: &[&str]) -> Result<Self> {
        if column_names.is_empty() {
            return Err(Error::IllegalArgument(
                "Column names cannot be empty".to_string(),
            ));
        }
        let row_type = self.table_info.row_type();
        let mut indices = Vec::new();

        for name in column_names {
            let idx = row_type
                .fields()
                .iter()
                .position(|f| f.name() == *name)
                .ok_or_else(|| Error::IllegalArgument(format!("Column '{name}' not found")))?;
            indices.push(idx);
        }

        self.projected_fields = Some(indices);
        Ok(self)
    }

    pub fn create_log_scanner(self) -> Result<LogScanner> {
        LogScanner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.projected_fields,
        )
    }
}

pub struct LogScanner {
    table_path: TablePath,
    table_id: i64,
    metadata: Arc<Metadata>,
    log_scanner_status: Arc<LogScannerStatus>,
    log_fetcher: LogFetcher,
}

impl LogScanner {
    pub fn new(
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

    pub async fn poll(&self, _timeout: Duration) -> Result<ScanRecords> {
        Ok(ScanRecords::new(self.poll_for_fetches().await?))
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        self.log_fetcher.send_fetches_and_collect().await
    }
}

#[allow(dead_code)]
struct LogFetcher {
    table_path: TablePath,
    conns: Arc<RpcClient>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    log_scanner_status: Arc<LogScannerStatus>,
    read_context: ReadContext,
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
        let full_arrow_schema = to_arrow_schema(table_info.get_row_type());
        let read_context = Self::create_read_context(full_arrow_schema, projected_fields.clone());

        let tmp_dir = TempDir::with_prefix("fluss-remote-logs")?;

        Ok(LogFetcher {
            table_path: table_info.table_path.clone(),
            conns,
            table_info,
            metadata,
            log_scanner_status,
            read_context,
            remote_log_downloader: Arc::new(RemoteLogDownloader::new(tmp_dir)?),
            credentials_cache: Arc::new(CredentialsCache::new()),
        })
    }

    fn create_read_context(
        full_arrow_schema: SchemaRef,
        projected_fields: Option<Vec<usize>>,
    ) -> ReadContext {
        match projected_fields {
            None => ReadContext::new(full_arrow_schema),
            Some(fields) => ReadContext::with_projection_pushdown(full_arrow_schema, fields),
        }
    }

    async fn send_fetches_and_collect(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let fetch_request = self.prepare_fetch_log_requests().await;
        eprintln!("[DEBUG] prepare_fetch_log_requests returned {} requests", fetch_request.len());
        let mut result: HashMap<TableBucket, Vec<ScanRecord>> = HashMap::new();
        for (leader, fetch_request) in fetch_request {
            eprintln!("[DEBUG] Sending fetch request to leader {}", leader);
            let cluster = self.metadata.get_cluster();
            let server_node = cluster
                .get_tablet_server(leader)
                .expect("todo: handle leader not exist.");
            let con = self.conns.get_connection(server_node).await?;

            eprintln!("[DEBUG] Sending RPC request...");
            let fetch_response = con
                .request(crate::rpc::message::FetchLogRequest::new(fetch_request))
                .await?;
            eprintln!("[DEBUG] Got RPC response with {} tables", fetch_response.tables_resp.len());

            for pb_fetch_log_resp in fetch_response.tables_resp {
                let table_id = pb_fetch_log_resp.table_id;
                let fetch_log_for_buckets = pb_fetch_log_resp.buckets_resp;

                for fetch_log_for_bucket in fetch_log_for_buckets {
                    let bucket: i32 = fetch_log_for_bucket.bucket_id;
                    let table_bucket = TableBucket::new(table_id, bucket);

                    // Check if this is a remote log fetch
                    if let Some(ref remote_log_fetch_info) =
                        fetch_log_for_bucket.remote_log_fetch_info
                    {
                        eprintln!("[DEBUG] Bucket {} requires remote log fetch with {} segments", 
                            bucket, remote_log_fetch_info.remote_log_segments.len());
                        eprintln!("[DEBUG] Getting S3 credentials...");
                        let s3_props =
                            self.credentials_cache.get_or_refresh(&self.conns, &self.metadata).await?;
                        eprintln!("[DEBUG] Got {} S3 props", s3_props.len());
                        self.remote_log_downloader.set_s3_props(s3_props);
                        let remote_fetch_info = RemoteLogFetchInfo::from_proto(
                            remote_log_fetch_info,
                            table_bucket.clone(),
                        )?;

                        if let Some(fetch_offset) =
                            self.log_scanner_status.get_bucket_offset(&table_bucket)
                        {
                            let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                            // Download and process remote log segments
                            let mut pos_in_log_segment = remote_fetch_info.first_start_pos;
                            let mut current_fetch_offset = fetch_offset;
                            // todo: make segment download parallelly
                            for (i, segment) in
                                remote_fetch_info.remote_log_segments.iter().enumerate()
                            {
                                if i > 0 {
                                    pos_in_log_segment = 0;
                                    current_fetch_offset = segment.start_offset;
                                }

                                eprintln!("[DEBUG] Downloading segment {} from {}", 
                                    segment.segment_id, remote_fetch_info.remote_log_tablet_dir);
                                let download_future =
                                    self.remote_log_downloader.request_remote_log(
                                        &remote_fetch_info.remote_log_tablet_dir,
                                        segment,
                                    )?;
                                let pending_fetch = RemotePendingFetch::new(
                                    segment.clone(),
                                    download_future,
                                    pos_in_log_segment,
                                    current_fetch_offset,
                                    high_watermark,
                                    self.read_context.clone(),
                                );
                                eprintln!("[DEBUG] Waiting for download to complete...");
                                let remote_records =
                                    pending_fetch.convert_to_completed_fetch().await?;
                                eprintln!("[DEBUG] Download completed, got {} records", 
                                    remote_records.values().map(|v| v.len()).sum::<usize>());
                                // Update offset and merge results
                                for (tb, records) in remote_records {
                                    if let Some(last_record) = records.last() {
                                        self.log_scanner_status
                                            .update_offset(&tb, last_record.offset() + 1);
                                    }
                                    result.entry(tb).or_default().extend(records);
                                }
                            }
                        } else {
                            // if the offset is null, it means the bucket has been unsubscribed,
                            // skip processing and continue to the next bucket.
                            continue;
                        }
                    } else if fetch_log_for_bucket.records.is_some() {
                        // Handle regular in-memory records
                        let mut fetch_records = vec![];
                        let data = fetch_log_for_bucket.records.unwrap();
                        for log_record in &mut LogRecordsBatchs::new(&data) {
                            let last_offset = log_record.last_log_offset();
                            fetch_records.extend(log_record.records(&self.read_context)?);
                            self.log_scanner_status
                                .update_offset(&table_bucket, last_offset + 1);
                        }
                        result.insert(table_bucket, fetch_records);
                    }
                }
            }
        }

        Ok(result)
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
                    // todo: debug
                    continue;
                }
            };

            if let Some(leader) = self.get_table_bucket_leader(&bucket) {
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
        // always available now
        self.log_scanner_status.fetchable_buckets(|_| true)
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
