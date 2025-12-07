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
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
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
}

impl RemoteLogDownloader {
    pub fn new(local_log_dir: TempDir) -> Result<Self> {
        Ok(Self { local_log_dir })
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
        // Spawn async download task
        tokio::spawn(async move {
            let result =
                Self::download_file(&remote_log_tablet_dir, &remote_path, &local_file_path).await;
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
    ) -> Result<PathBuf> {
        // Handle both URL (e.g., "s3://bucket/path") and local file paths
        // If the path doesn't contain "://", treat it as a local file path
        let remote_log_tablet_dir_url = if remote_log_tablet_dir.contains("://") {
            remote_log_tablet_dir.to_string()
        } else {
            format!("file://{remote_log_tablet_dir}")
        };

        // Create FileIO from the remote log tablet dir URL to get the storage
        let file_io_builder = FileIO::from_url(&remote_log_tablet_dir_url)?;

        // Build storage and create operator directly
        let storage = Storage::build(file_io_builder)?;
        let (op, relative_path) = storage.create(remote_path)?;

        // Get file metadata to know the size
        let meta = op.stat(relative_path).await?;
        let file_size = meta.content_length();

        // Create local file for writing
        let mut local_file = tokio::fs::File::create(local_path).await?;

        // Stream data from remote to local file in chunks
        // opendal::Reader::read accepts a range, so we read in chunks
        const CHUNK_SIZE: u64 = 64 * 1024; // 64KB chunks for efficient streaming
        let mut offset = 0u64;

        while offset < file_size {
            let end = std::cmp::min(offset + CHUNK_SIZE, file_size);
            let range = offset..end;

            // Read chunk from remote storage
            let chunk = op.read_with(relative_path).range(range.clone()).await?;
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

        // Parse log records
        let mut fetch_records = vec![];
        for log_record in &mut LogRecordsBatchs::new(data) {
            fetch_records.extend(log_record.records(&self.read_context)?);
        }

        let mut result = HashMap::new();
        result.insert(self.segment.table_bucket.clone(), fetch_records);
        Ok(result)
    }
}
