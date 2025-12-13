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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::{Error, Result};
use crate::io::{FileIO, Storage};
use crate::metadata::TableBucket;
use crate::proto::{PbRemoteLogFetchInfo, PbRemoteLogSegment};
use crate::record::{LogRecordsBatchs, ReadContext, ScanRecord};
use crate::util::delete_file;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

/// Represents a remote log segment that needs to be downloaded
#[derive(Debug, Clone)]
pub struct RemoteLogSegment {
    pub segment_id: String,
    pub start_offset: i64,
    #[allow(dead_code)]
    pub end_offset: i64,
    #[allow(dead_code)]
    pub size_in_bytes: i32,
    pub table_bucket: TableBucket,
}

impl RemoteLogSegment {
    pub fn from_proto(segment: &PbRemoteLogSegment, table_bucket: TableBucket) -> Self {
        Self {
            segment_id: segment.remote_log_segment_id.clone(),
            start_offset: segment.remote_log_start_offset,
            end_offset: segment.remote_log_end_offset,
            size_in_bytes: segment.segment_size_in_bytes,
            table_bucket,
        }
    }

    /// Get the local file name for this remote log segment
    pub fn local_file_name(&self) -> String {
        // Format: ${remote_segment_id}_${offset_prefix}.log
        let offset_prefix = format!("{:020}", self.start_offset);
        format!("{}_{}.log", self.segment_id, offset_prefix)
    }
}

/// Represents remote log fetch information
#[derive(Debug, Clone)]
pub struct RemoteLogFetchInfo {
    pub remote_log_tablet_dir: String,
    #[allow(dead_code)]
    pub partition_name: Option<String>,
    pub remote_log_segments: Vec<RemoteLogSegment>,
    pub first_start_pos: i32,
}

impl RemoteLogFetchInfo {
    pub fn from_proto(info: &PbRemoteLogFetchInfo, table_bucket: TableBucket) -> Result<Self> {
        let segments = info
            .remote_log_segments
            .iter()
            .map(|s| RemoteLogSegment::from_proto(s, table_bucket.clone()))
            .collect();

        Ok(Self {
            remote_log_tablet_dir: info.remote_log_tablet_dir.clone(),
            partition_name: info.partition_name.clone(),
            remote_log_segments: segments,
            first_start_pos: info.first_start_pos.unwrap_or(0),
        })
    }
}

/// Future for a remote log download request
pub struct RemoteLogDownloadFuture {
    receiver: Option<oneshot::Receiver<Result<PathBuf>>>,
}

impl RemoteLogDownloadFuture {
    pub fn new(receiver: oneshot::Receiver<Result<PathBuf>>) -> Self {
        Self {
            receiver: Some(receiver),
        }
    }

    /// Get the downloaded file path
    pub async fn get_file_path(&mut self) -> Result<PathBuf> {
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| Error::Io(io::Error::other("Download future already consumed")))?;

        receiver.await.map_err(|e| {
            Error::Io(io::Error::other(format!(
                "Download future cancelled: {e:?}"
            )))
        })?
    }
}

/// Downloader for remote log segment files
pub struct RemoteLogDownloader {
    local_log_dir: TempDir,
    s3_props: Arc<RwLock<HashMap<String, String>>>,
}

impl RemoteLogDownloader {
    pub fn new(local_log_dir: TempDir) -> Result<Self> {
        Ok(Self {
            local_log_dir,
            s3_props: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn set_s3_props(&self, props: HashMap<String, String>) {
        *self.s3_props.write() = props;
    }

    /// Request to fetch a remote log segment to local. This method is non-blocking.
    pub fn request_remote_log(
        &self,
        remote_log_tablet_dir: &str,
        segment: &RemoteLogSegment,
    ) -> Result<RemoteLogDownloadFuture> {
        let (sender, receiver) = oneshot::channel();
        let local_file_name = segment.local_file_name();
        let local_file_path = self.local_log_dir.path().join(&local_file_name);
        let remote_path = self.build_remote_path(remote_log_tablet_dir, segment);
        let remote_log_tablet_dir = remote_log_tablet_dir.to_string();
        let s3_props = self.s3_props.read().clone();
        // Spawn async download task
        tokio::spawn(async move {
            let result = Self::download_file(
                &remote_log_tablet_dir,
                &remote_path,
                &local_file_path,
                &s3_props,
            )
            .await;
            let _ = sender.send(result);
        });
        Ok(RemoteLogDownloadFuture::new(receiver))
    }

    /// Build the remote path for a log segment
    fn build_remote_path(&self, remote_log_tablet_dir: &str, segment: &RemoteLogSegment) -> String {
        // Format: ${remote_log_tablet_dir}/${segment_id}/${offset_prefix}.log
        let offset_prefix = format!("{:020}", segment.start_offset);
        format!(
            "{}/{}/{}.log",
            remote_log_tablet_dir, segment.segment_id, offset_prefix
        )
    }

    /// Download a file from remote storage to local using streaming read/write
    async fn download_file(
        remote_log_tablet_dir: &str,
        remote_path: &str,
        local_path: &Path,
        s3_props: &HashMap<String, String>,
    ) -> Result<PathBuf> {
        eprintln!("[DEBUG] download_file called: remote_path={}", remote_path);
        eprintln!("[DEBUG] s3_props count: {}", s3_props.len());
        for (k, v) in s3_props {
            if k.contains("key") || k.contains("secret") {
                eprintln!("[DEBUG]   {} = {}...", k, &v[..std::cmp::min(8, v.len())]);
            } else {
                eprintln!("[DEBUG]   {} = {}", k, v);
            }
        }
        
        // Handle both URL (e.g., "s3://bucket/path") and local file paths
        // If the path doesn't contain "://", treat it as a local file path
        let remote_log_tablet_dir_url = if remote_log_tablet_dir.contains("://") {
            remote_log_tablet_dir.to_string()
        } else {
            format!("file://{remote_log_tablet_dir}")
        };

        // Create FileIO from the remote log tablet dir URL to get the storage
        let file_io_builder = FileIO::from_url(&remote_log_tablet_dir_url)?;

        // For S3/S3A URLs, inject S3 credentials from props
        let file_io_builder = if remote_log_tablet_dir.starts_with("s3://")
            || remote_log_tablet_dir.starts_with("s3a://")
        {
            file_io_builder.with_props(s3_props.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        } else {
            file_io_builder
        };

        // Build storage and create operator directly
        let storage = Storage::build(file_io_builder)?;
        let (op, relative_path) = storage.create(remote_path)?;

        // Timeout for remote storage operations (30 seconds)
        const REMOTE_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

        eprintln!("[DEBUG] Calling stat on: {}", relative_path);
        // Get file metadata to know the size with timeout
        let stat_future = op.stat(relative_path);
        let meta = tokio::time::timeout(REMOTE_OP_TIMEOUT, stat_future)
            .await
            .map_err(|_| Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Timeout getting file metadata from remote storage: {}", remote_path)
            )))??;
        let file_size = meta.content_length();
        eprintln!("[DEBUG] stat succeeded, file_size={}", file_size);

        // Create local file for writing
        let mut local_file = tokio::fs::File::create(local_path).await?;

        // Stream data from remote to local file in chunks
        // opendal::Reader::read accepts a range, so we read in chunks
        const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8MB chunks for efficient streaming
        let mut offset = 0u64;
        let mut chunk_count = 0u64;
        let total_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        eprintln!("[DEBUG] Starting download: {} bytes in {} chunks", file_size, total_chunks);

        while offset < file_size {
            let end = std::cmp::min(offset + CHUNK_SIZE, file_size);
            let range = offset..end;
            chunk_count += 1;

            if chunk_count <= 3 || chunk_count % 10 == 0 {
                eprintln!("[DEBUG] Reading chunk {}/{} (offset {})", chunk_count, total_chunks, offset);
            }

            // Read chunk from remote storage with timeout
            let read_future = op.read_with(relative_path).range(range.clone());
            let chunk = tokio::time::timeout(REMOTE_OP_TIMEOUT, read_future)
                .await
                .map_err(|_| Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("Timeout reading chunk from remote storage: {} at offset {}", remote_path, offset)
                )))??;
            let bytes = chunk.to_bytes();

            // Write chunk to local file
            local_file.write_all(&bytes).await?;

            offset = end;
        }

        // Ensure all data is flushed to disk
        local_file.sync_all().await?;

        Ok(local_path.to_path_buf())
    }
}

/// Pending fetch that waits for remote log file to be downloaded
pub struct RemotePendingFetch {
    segment: RemoteLogSegment,
    download_future: RemoteLogDownloadFuture,
    pos_in_log_segment: i32,
    #[allow(dead_code)]
    fetch_offset: i64,
    #[allow(dead_code)]
    high_watermark: i64,
    read_context: ReadContext,
}

impl RemotePendingFetch {
    pub fn new(
        segment: RemoteLogSegment,
        download_future: RemoteLogDownloadFuture,
        pos_in_log_segment: i32,
        fetch_offset: i64,
        high_watermark: i64,
        read_context: ReadContext,
    ) -> Self {
        Self {
            segment,
            download_future,
            pos_in_log_segment,
            fetch_offset,
            high_watermark,
            read_context,
        }
    }

    /// Convert to completed fetch by reading the downloaded file
    pub async fn convert_to_completed_fetch(
        mut self,
    ) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let file_path = self.download_future.get_file_path().await?;
        let file_data = tokio::fs::read(&file_path).await?;

        // Slice the data if needed
        let data = if self.pos_in_log_segment > 0 {
            &file_data[self.pos_in_log_segment as usize..]
        } else {
            &file_data
        };

        // delete the downloaded local file to free disk
        delete_file(file_path).await;

        // Parse log records (remote log contains full data, need client-side projection)
        let mut fetch_records = vec![];
        for log_record in &mut LogRecordsBatchs::new(data) {
            fetch_records.extend(log_record.records_for_remote_log(&self.read_context)?);
        }

        let mut result = HashMap::new();
        result.insert(self.segment.table_bucket.clone(), fetch_records);
        Ok(result)
    }
}
