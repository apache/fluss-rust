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

use clap::{ArgAction, Parser};
use serde::{Deserialize, Serialize};

const DEFAULT_BOOTSTRAP_SERVER: &str = "127.0.0.1:9123";
const DEFAULT_REQUEST_MAX_SIZE: i32 = 10 * 1024 * 1024;
const DEFAULT_WRITER_BATCH_SIZE: i32 = 2 * 1024 * 1024;
const DEFAULT_RETRIES: i32 = i32::MAX;
const DEFAULT_PREFETCH_NUM: usize = 4;
const DEFAULT_DOWNLOAD_THREADS: usize = 3;
const DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ: bool = true;
const DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ_CONCURRENCY: usize = 4;

const DEFAULT_ACKS: &str = "all";

fn default_scanner_remote_log_streaming_read() -> bool {
    DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ
}

fn default_scanner_remote_log_streaming_read_concurrency() -> usize {
    DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ_CONCURRENCY
}

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value_t = String::from(DEFAULT_BOOTSTRAP_SERVER))]
    pub bootstrap_servers: String,

    #[arg(long, default_value_t = DEFAULT_REQUEST_MAX_SIZE)]
    pub writer_request_max_size: i32,

    #[arg(long, default_value_t = String::from(DEFAULT_ACKS))]
    pub writer_acks: String,

    #[arg(long, default_value_t = DEFAULT_RETRIES)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = DEFAULT_WRITER_BATCH_SIZE)]
    pub writer_batch_size: i32,

    /// Maximum number of remote log segments to prefetch
    /// Default: 4 (matching Java CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM)
    #[arg(long, default_value_t = DEFAULT_PREFETCH_NUM)]
    pub scanner_remote_log_prefetch_num: usize,

    /// Maximum concurrent remote log downloads
    /// Default: 3 (matching Java REMOTE_FILE_DOWNLOAD_THREAD_NUM)
    #[arg(long, default_value_t = DEFAULT_DOWNLOAD_THREADS)]
    pub remote_file_download_thread_num: usize,

    /// Whether to use opendal streaming reader path for remote log downloads.
    #[arg(
        long,
        default_value_t = DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ,
        action = ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    #[serde(default = "default_scanner_remote_log_streaming_read")]
    pub scanner_remote_log_streaming_read: bool,

    /// Intra-file streaming read concurrency for each remote segment download.
    #[arg(
        long,
        default_value_t = DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ_CONCURRENCY
    )]
    #[serde(default = "default_scanner_remote_log_streaming_read_concurrency")]
    pub scanner_remote_log_streaming_read_concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from(DEFAULT_BOOTSTRAP_SERVER),
            writer_request_max_size: DEFAULT_REQUEST_MAX_SIZE,
            writer_acks: String::from(DEFAULT_ACKS),
            writer_retries: i32::MAX,
            writer_batch_size: DEFAULT_WRITER_BATCH_SIZE,
            scanner_remote_log_prefetch_num: DEFAULT_PREFETCH_NUM,
            remote_file_download_thread_num: DEFAULT_DOWNLOAD_THREADS,
            scanner_remote_log_streaming_read: DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ,
            scanner_remote_log_streaming_read_concurrency:
                DEFAULT_SCANNER_REMOTE_LOG_STREAMING_READ_CONCURRENCY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use clap::Parser;

    #[test]
    fn parse_streaming_read_defaults_to_true() {
        let config = Config::parse_from(["prog"]);
        assert!(config.scanner_remote_log_streaming_read);
    }

    #[test]
    fn parse_streaming_read_accepts_false() {
        let config = Config::parse_from(["prog", "--scanner-remote-log-streaming-read", "false"]);
        assert!(!config.scanner_remote_log_streaming_read);
    }

    #[test]
    fn parse_streaming_read_flag_without_value_means_true() {
        let config = Config::parse_from(["prog", "--scanner-remote-log-streaming-read"]);
        assert!(config.scanner_remote_log_streaming_read);
    }
}
