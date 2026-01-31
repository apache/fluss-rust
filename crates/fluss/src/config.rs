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

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_server: Option<String>,

    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub request_max_size: i32,

    #[arg(long, default_value_t = String::from("all"))]
    pub writer_acks: String,

    #[arg(long, default_value_t = i32::MAX)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub writer_batch_size: i32,

    /// Maximum number of remote log segments to prefetch
    /// Default: 4 (matching Java CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM)
    #[arg(long, default_value_t = 4)]
    pub scanner_remote_log_prefetch_num: usize,

    /// Maximum concurrent remote log downloads
    /// Default: 3 (matching Java REMOTE_FILE_DOWNLOAD_THREAD_NUM)
    #[arg(long, default_value_t = 3)]
    pub scanner_remote_log_download_threads: usize,

    /// Maximum number of pending lookup operations
    /// Default: 25600 (matching Java CLIENT_LOOKUP_QUEUE_SIZE)
    #[arg(long, default_value_t = 25600)]
    pub lookup_queue_size: usize,

    /// Maximum batch size of merging lookup operations to one lookup request
    /// Default: 128 (matching Java CLIENT_LOOKUP_MAX_BATCH_SIZE)
    #[arg(long, default_value_t = 128)]
    pub lookup_max_batch_size: usize,

    /// Maximum time to wait for the lookup batch to fill (in milliseconds)
    /// Default: 100 (matching Java CLIENT_LOOKUP_BATCH_TIMEOUT)
    #[arg(long, default_value_t = 100)]
    pub lookup_batch_timeout_ms: u64,

    /// Maximum number of unacknowledged lookup requests
    /// Default: 128 (matching Java CLIENT_LOOKUP_MAX_INFLIGHT_SIZE)
    #[arg(long, default_value_t = 128)]
    pub lookup_max_inflight_requests: usize,

    /// Maximum number of lookup retries
    /// Default: i32::MAX (matching Java CLIENT_LOOKUP_MAX_RETRIES)
    #[arg(long, default_value_t = i32::MAX)]
    pub lookup_max_retries: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_server: None,
            request_max_size: 10 * 1024 * 1024,
            writer_acks: String::from("all"),
            writer_retries: i32::MAX,
            writer_batch_size: 2 * 1024 * 1024,
            scanner_remote_log_prefetch_num: 4,
            scanner_remote_log_download_threads: 3,
            lookup_queue_size: 25600,
            lookup_max_batch_size: 128,
            lookup_batch_timeout_ms: 100,
            lookup_max_inflight_requests: 128,
            lookup_max_retries: i32::MAX,
        }
    }
}
