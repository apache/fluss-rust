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

use crate::client::table::log_fetch_buffer::{CompletedFetch, DefaultCompletedFetch, PendingFetch};
use crate::error::{Error, Result};
use crate::io::{FileIO, Storage};
use crate::metadata::TableBucket;
use crate::proto::{PbRemoteLogFetchInfo, PbRemoteLogSegment};
use crate::record::{LogRecordsBatches, ReadContext};
use crate::util::delete_file;
use parking_lot::{Mutex, RwLock};
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
    pub fn from_proto(info: &PbRemoteLogFetchInfo, table_bucket: TableBucket) -> Self {
        let segments = info
            .remote_log_segments
            .iter()
            .map(|s| RemoteLogSegment::from_proto(s, table_bucket.clone()))
            .collect();

        Self {
            remote_log_tablet_dir: info.remote_log_tablet_dir.clone(),
            partition_name: info.partition_name.clone(),
            remote_log_segments: segments,
            first_start_pos: info.first_start_pos.unwrap_or(0),
        }
    }
}

type CompletionCallback = Box<dyn Fn() + Send + Sync>;

/// Future for a remote log download request
pub struct RemoteLogDownloadFuture {
    result: Arc<Mutex<Option<Result<Vec<u8>>>>>,
    completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>>,
    // todo: add recycleCallback
}

impl RemoteLogDownloadFuture {
    pub fn new(receiver: oneshot::Receiver<Result<Vec<u8>>>) -> Self {
        let result = Arc::new(Mutex::new(None));
        let result_clone = Arc::clone(&result);
        let completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>> =
            Arc::new(Mutex::new(Vec::new()));
        let callbacks_clone = Arc::clone(&completion_callbacks);

        // Spawn a task to wait for the download and update result, then call callbacks
        tokio::spawn(async move {
            let download_result = match receiver.await {
                Ok(Ok(path)) => Ok(path),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Error::UnexpectedError {
                    message: format!("Download & Read future cancelled: {e:?}"),
                    source: None,
                }),
            };

            *result_clone.lock() = Some(download_result);

            // Call all registered callbacks
            // We need to take the callbacks to avoid holding the lock while calling them
            // This also ensures that any callbacks registered after this point will be called immediately
            let callbacks: Vec<CompletionCallback> = {
                let mut callbacks_guard = callbacks_clone.lock();
                std::mem::take(&mut *callbacks_guard)
            };
            for callback in callbacks {
                callback();
            }

            // After calling callbacks, any new callbacks registered will see is_done() == true
            // and will be called immediately in on_complete()
        });

        Self {
            result,
            completion_callbacks,
        }
    }

    /// Register a callback to be called when download completes (similar to Java's onComplete)
    pub fn on_complete<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // Acquire callbacks lock first to ensure atomicity of the check-and-register operation
        let mut callbacks_guard = self.completion_callbacks.lock();

        // Check completion status while holding the callbacks lock.
        // This ensures that:
        // 1. If the task completes between checking is_done() and registering the callback,
        //    we'll see the completion state correctly
        // 2. The background task cannot clear the callbacks list while we're checking/registering
        let is_done = self.result.lock().is_some();

        if is_done {
            // If already completed, call immediately (drop lock first to avoid deadlock)
            drop(callbacks_guard);
            callback();
        } else {
            // Register the callback while holding the callbacks lock.
            // This ensures that even if the background task completes right after we check
            // is_done(), it will wait for us to release the lock before taking callbacks.
            // When it does take callbacks, it will see our callback in the list and execute it.
            callbacks_guard.push(Box::new(callback));
            // Lock is automatically released here
        }
    }

    pub fn is_done(&self) -> bool {
        self.result.lock().is_some()
    }

    /// Get the downloaded file path (synchronous, only works after is_done() returns true)
    pub fn get_remote_log_bytes(&self) -> Result<Vec<u8>> {
        // todo: handle download fail
        let guard = self.result.lock();
        match guard.as_ref() {
            Some(Ok(path)) => Ok(path.clone()),
            Some(Err(e)) => Err(Error::IoUnexpectedError {
                message: format!("Fail to get remote log bytes: {e}"),
                source: io::Error::other(format!("{e:?}")),
            }),
            None => Err(Error::IoUnexpectedError {
                message: "Get remote log bytes not completed yet".to_string(),
                source: io::Error::other("Get remote log bytes not completed yet"),
            }),
        }
    }
}

/// Downloader for remote log segment files
pub struct RemoteLogDownloader {
    local_log_dir: TempDir,
    remote_fs_props: RwLock<HashMap<String, String>>,
}

impl RemoteLogDownloader {
    pub fn new(local_log_dir: TempDir) -> Result<Self> {
        Ok(Self {
            local_log_dir,
            remote_fs_props: RwLock::new(HashMap::new()),
        })
    }

    pub fn set_remote_fs_props(&self, props: HashMap<String, String>) {
        *self.remote_fs_props.write() = props;
    }

    /// Request to fetch a remote log segment to local. This method is non-blocking.
    pub fn request_remote_log(
        &self,
        remote_log_tablet_dir: &str,
        segment: &RemoteLogSegment,
    ) -> RemoteLogDownloadFuture {
        let (sender, receiver) = oneshot::channel();
        let local_file_name = segment.local_file_name();
        let local_file_path = self.local_log_dir.path().join(&local_file_name);
        let remote_path = self.build_remote_path(remote_log_tablet_dir, segment);
        let remote_log_tablet_dir = remote_log_tablet_dir.to_string();
        let remote_fs_props = self.remote_fs_props.read().clone();
        // Spawn async download & read task
        tokio::spawn(async move {
            let result = async {
                let file_path = Self::download_file(
                    &remote_log_tablet_dir,
                    &remote_path,
                    &local_file_path,
                    &remote_fs_props,
                )
                .await?;
                let bytes = tokio::fs::read(&file_path).await?;

                // Delete the downloaded local file to free disk (async, but we'll do it in background)
                let file_path_clone = file_path.clone();
                tokio::spawn(async move {
                    let _ = delete_file(file_path_clone).await;
                });

                Ok(bytes)
            }
            .await;

            let _ = sender.send(result);
        });
        RemoteLogDownloadFuture::new(receiver)
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
        remote_fs_props: &HashMap<String, String>,
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

        // For S3/S3A URLs, inject S3 credentials from props
        let file_io_builder = if remote_log_tablet_dir.starts_with("s3://")
            || remote_log_tablet_dir.starts_with("s3a://")
        {
            file_io_builder.with_props(
                remote_fs_props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str())),
            )
        } else {
            file_io_builder
        };

        // Build storage and create operator directly
        let storage = Storage::build(file_io_builder)?;
        let (op, relative_path) = storage.create(remote_path)?;

        // Timeout for remote storage operations (30 seconds)
        const REMOTE_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

        // Get file metadata to know the size with timeout
        let meta = op.stat(relative_path).await?;
        let file_size = meta.content_length();

        // Create local file for writing
        let mut local_file = tokio::fs::File::create(local_path).await?;

        // Stream data from remote to local file in chunks
        // opendal::Reader::read accepts a range, so we read in chunks
        const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8MB chunks for efficient reading
        let mut offset = 0u64;
        let mut chunk_count = 0u64;
        let total_chunks = file_size.div_ceil(CHUNK_SIZE);

        while offset < file_size {
            let end = std::cmp::min(offset + CHUNK_SIZE, file_size);
            let range = offset..end;
            chunk_count += 1;

            if chunk_count <= 3 || chunk_count % 10 == 0 {
                log::debug!(
                    "Remote log download: reading chunk {chunk_count}/{total_chunks} (offset {offset})"
                );
            }

            // Read chunk from remote storage with timeout
            let read_future = op.read_with(relative_path).range(range.clone());
            let chunk = tokio::time::timeout(REMOTE_OP_TIMEOUT, read_future)
                .await
                .map_err(|e| {
                    Error::IoUnexpectedError {
                        message: format!(
                            "Timeout reading chunk from remote storage: {remote_path} at offset {offset}, exception: {e}."
                        ),
                        source: io::ErrorKind::TimedOut.into(),
                    }
                })??;
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
    fetch_offset: i64,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{DataField, DataTypes, TablePath};
    use crate::record::{MemoryLogRecordsArrowBuilder, to_arrow_schema};
    use crate::row::{Datum, GenericRow};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::time::{Duration, timeout};

    fn build_log_bytes() -> Result<Vec<u8>> {
        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
        );
        let record = WriteRecord::new(
            table_path,
            GenericRow {
                values: vec![Datum::Int32(1)],
            },
        );
        builder.append(&record)?;
        builder.build()
    }

    #[test]
    fn remote_log_segment_local_file_name() {
        let bucket = TableBucket::new(1, 0);
        let segment = RemoteLogSegment {
            segment_id: "seg".to_string(),
            start_offset: 12,
            end_offset: 20,
            size_in_bytes: 0,
            table_bucket: bucket,
        };
        assert_eq!(segment.local_file_name(), "seg_00000000000000000012.log");
    }

    #[test]
    fn remote_log_fetch_info_from_proto_defaults_start_pos() {
        let bucket = TableBucket::new(1, 0);
        let proto_segment = PbRemoteLogSegment {
            remote_log_segment_id: "seg".to_string(),
            remote_log_start_offset: 0,
            remote_log_end_offset: 10,
            segment_size_in_bytes: 1,
        };
        let proto_info = PbRemoteLogFetchInfo {
            remote_log_tablet_dir: "/tmp/remote".to_string(),
            partition_name: None,
            remote_log_segments: vec![proto_segment],
            first_start_pos: None,
        };
        let info = RemoteLogFetchInfo::from_proto(&proto_info, bucket.clone());
        assert_eq!(info.first_start_pos, 0);
        assert_eq!(info.remote_log_segments.len(), 1);
        assert_eq!(info.remote_log_segments[0].table_bucket, bucket);
    }

    #[tokio::test]
    async fn download_future_callbacks_fire() -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let future = RemoteLogDownloadFuture::new(rx);
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = fired.clone();
        future.on_complete(move || {
            fired_clone.store(true, Ordering::Release);
        });
        tx.send(Ok(vec![1, 2, 3])).unwrap();

        let _ = timeout(Duration::from_millis(50), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        assert!(fired.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn download_future_get_bytes_errors() -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let future = RemoteLogDownloadFuture::new(rx);
        assert!(matches!(
            future.get_remote_log_bytes(),
            Err(Error::IoUnexpectedError { .. })
        ));
        tx.send(Err(Error::UnexpectedError {
            message: "boom".to_string(),
            source: None,
        }))
        .unwrap();

        let _ = timeout(Duration::from_millis(50), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        let err = future.get_remote_log_bytes().unwrap_err();
        assert!(matches!(err, Error::IoUnexpectedError { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn download_future_canceled_returns_error() -> Result<()> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>>>();
        drop(tx);
        let future = RemoteLogDownloadFuture::new(rx);
        let _ = timeout(Duration::from_millis(50), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        let err = future.get_remote_log_bytes().unwrap_err();
        assert!(matches!(err, Error::IoUnexpectedError { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn download_file_rejects_invalid_url() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let local_path = temp_dir.path().join("local.log");
        let result = RemoteLogDownloader::download_file(
            "://",
            "/tmp/missing.log",
            &local_path,
            &HashMap::new(),
        )
        .await;
        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn download_file_missing_remote_path_returns_error() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let remote_dir = temp_dir.path();
        let local_path = temp_dir.path().join("local.log");
        let remote_path = remote_dir.join("missing.log");
        let result = RemoteLogDownloader::download_file(
            remote_dir.to_str().unwrap(),
            remote_path.to_str().unwrap(),
            &local_path,
            &HashMap::new(),
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::RemoteStorageUnexpectedError { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn request_remote_log_reads_local_file() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let remote_dir = temp_dir.path().join("remote");
        tokio::fs::create_dir_all(&remote_dir).await?;

        let bucket = TableBucket::new(1, 0);
        let segment = RemoteLogSegment {
            segment_id: "seg".to_string(),
            start_offset: 0,
            end_offset: 0,
            size_in_bytes: 0,
            table_bucket: bucket,
        };

        let downloader = RemoteLogDownloader::new(TempDir::new()?)?;
        let remote_path = downloader.build_remote_path(remote_dir.to_str().unwrap(), &segment);
        let remote_file = PathBuf::from(&remote_path);
        if let Some(parent) = remote_file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&remote_file, b"data").await?;

        let future = downloader.request_remote_log(remote_dir.to_str().unwrap(), &segment);
        let _ = timeout(Duration::from_millis(200), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        let bytes = future.get_remote_log_bytes()?;
        assert_eq!(bytes, b"data");
        Ok(())
    }

    #[tokio::test]
    async fn remote_pending_fetch_to_completed_fetch() -> Result<()> {
        let bytes = build_log_bytes()?;
        let (tx, rx) = oneshot::channel();
        tx.send(Ok(bytes)).unwrap();
        let future = RemoteLogDownloadFuture::new(rx);
        let _ = timeout(Duration::from_millis(50), async {
            while !future.is_done() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type), false);

        let bucket = TableBucket::new(1, 0);
        let segment = RemoteLogSegment {
            segment_id: "seg".to_string(),
            start_offset: 0,
            end_offset: 0,
            size_in_bytes: 0,
            table_bucket: bucket.clone(),
        };

        let pending = RemotePendingFetch::new(segment, future, 0, 0, 0, read_context);
        let mut completed = Box::new(pending).to_completed_fetch()?;
        let records = completed.fetch_records(10)?;
        assert_eq!(records.len(), 1);
        Ok(())
    }
}

impl PendingFetch for RemotePendingFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.segment.table_bucket
    }

    fn is_completed(&self) -> bool {
        self.download_future.is_done()
    }

    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
        // Get the file path (this should only be called when is_completed() returns true)
        let mut data = self.download_future.get_remote_log_bytes()?;

        // Slice the data if needed
        let data = if self.pos_in_log_segment > 0 {
            data.split_off(self.pos_in_log_segment as usize)
        } else {
            data
        };

        let size_in_bytes = data.len();

        let log_record_batch = LogRecordsBatches::new(data);

        // Create DefaultCompletedFetch from the data
        let completed_fetch = DefaultCompletedFetch::new(
            self.segment.table_bucket,
            log_record_batch,
            size_in_bytes,
            self.read_context,
            self.fetch_offset,
            self.high_watermark,
        );

        Ok(Box::new(completed_fetch))
    }
}
