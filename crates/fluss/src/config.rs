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

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::fmt;

const DEFAULT_BOOTSTRAP_SERVER: &str = "127.0.0.1:9123";
const DEFAULT_REQUEST_MAX_SIZE: i32 = 10 * 1024 * 1024;
const DEFAULT_WRITER_BATCH_SIZE: i32 = 2 * 1024 * 1024;
const DEFAULT_RETRIES: i32 = i32::MAX;
const DEFAULT_PREFETCH_NUM: usize = 4;
const DEFAULT_DOWNLOAD_THREADS: usize = 3;
const DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY: usize = 4;
const DEFAULT_MAX_POLL_RECORDS: usize = 500;
const DEFAULT_WRITER_BATCH_TIMEOUT_MS: i64 = 100;

const DEFAULT_ACKS: &str = "all";

fn default_scanner_remote_log_read_concurrency() -> usize {
    DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY
}

/// Bucket assigner strategy for tables without bucket keys.
/// Matches Java `client.writer.bucket.no-key-assigner`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoKeyAssigner {
    /// Sticks to one bucket until the batch is full, then switches.
    Sticky,
    /// Assigns each record to the next bucket in a rotating sequence.
    RoundRobin,
}

impl fmt::Display for NoKeyAssigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoKeyAssigner::Sticky => write!(f, "sticky"),
            NoKeyAssigner::RoundRobin => write!(f, "round_robin"),
        }
    }
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

    #[arg(long, value_enum, default_value_t = NoKeyAssigner::Sticky)]
    pub writer_bucket_no_key_assigner: NoKeyAssigner,

    /// Maximum number of remote log segments to prefetch
    /// Default: 4 (matching Java CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM)
    #[arg(long, default_value_t = DEFAULT_PREFETCH_NUM)]
    pub scanner_remote_log_prefetch_num: usize,

    /// Maximum concurrent remote log downloads
    /// Default: 3 (matching Java REMOTE_FILE_DOWNLOAD_THREAD_NUM)
    #[arg(long, default_value_t = DEFAULT_DOWNLOAD_THREADS)]
    pub remote_file_download_thread_num: usize,

    /// Intra-file remote log read concurrency for each remote segment download.
    /// Download path always uses streaming reader.
    #[arg(long, default_value_t = DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY)]
    #[serde(default = "default_scanner_remote_log_read_concurrency")]
    pub scanner_remote_log_read_concurrency: usize,

    /// Maximum number of records returned in a single call to poll() for LogScanner.
    /// Default: 500 (matching Java CLIENT_SCANNER_LOG_MAX_POLL_RECORDS)
    #[arg(long, default_value_t = DEFAULT_MAX_POLL_RECORDS)]
    pub scanner_log_max_poll_records: usize,

    /// The maximum time to wait for a batch to be completed in milliseconds.
    /// Default: 100 (matching Java CLIENT_WRITER_BATCH_TIMEOUT)
    #[arg(long, default_value_t = DEFAULT_WRITER_BATCH_TIMEOUT_MS)]
    pub writer_batch_timeout_ms: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from(DEFAULT_BOOTSTRAP_SERVER),
            writer_request_max_size: DEFAULT_REQUEST_MAX_SIZE,
            writer_acks: String::from(DEFAULT_ACKS),
            writer_retries: i32::MAX,
            writer_batch_size: DEFAULT_WRITER_BATCH_SIZE,
            writer_bucket_no_key_assigner: NoKeyAssigner::Sticky,
            scanner_remote_log_prefetch_num: DEFAULT_PREFETCH_NUM,
            remote_file_download_thread_num: DEFAULT_DOWNLOAD_THREADS,
            scanner_remote_log_read_concurrency: DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY,
            scanner_log_max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            writer_batch_timeout_ms: DEFAULT_WRITER_BATCH_TIMEOUT_MS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use clap::Parser;

    #[test]
    fn parse_remote_log_read_concurrency_defaults_to_four() {
        let config = Config::parse_from(["prog"]);
        assert_eq!(config.scanner_remote_log_read_concurrency, 4);
    }

    #[test]
    fn parse_remote_log_read_concurrency() {
        let config = Config::parse_from(["prog", "--scanner-remote-log-read-concurrency", "8"]);
        assert_eq!(config.scanner_remote_log_read_concurrency, 8);
    }
}
