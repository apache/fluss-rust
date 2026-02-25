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

use crate::*;
use pyo3::types::PyDict;

/// Configuration for Fluss client
#[pyclass]
#[derive(Clone)]
pub struct Config {
    inner: fcore::config::Config,
}

#[pymethods]
impl Config {
    /// Create a new Config with optional properties from a dictionary
    #[new]
    #[pyo3(signature = (properties = None))]
    fn new(properties: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut config = fcore::config::Config::default();

        if let Some(props) = properties {
            for item in props.iter() {
                let key: String = item.0.extract()?;
                let value: String = item.1.extract()?;

                match key.as_str() {
                    "bootstrap.servers" => {
                        config.bootstrap_servers = value;
                    }
                    "writer.request-max-size" => {
                        config.writer_request_max_size = value.parse::<i32>().map_err(|e| {
                            FlussError::new_err(format!("Invalid value '{value}' for '{key}': {e}"))
                        })?;
                    }
                    "writer.acks" => {
                        config.writer_acks = value;
                    }
                    "writer.retries" => {
                        config.writer_retries = value.parse::<i32>().map_err(|e| {
                            FlussError::new_err(format!("Invalid value '{value}' for '{key}': {e}"))
                        })?;
                    }
                    "writer.batch-size" => {
                        config.writer_batch_size = value.parse::<i32>().map_err(|e| {
                            FlussError::new_err(format!("Invalid value '{value}' for '{key}': {e}"))
                        })?;
                    }
                    "writer.batch-timeout-ms" => {
                        config.writer_batch_timeout_ms = value.parse::<i64>().map_err(|e| {
                            FlussError::new_err(format!("Invalid value '{value}' for '{key}': {e}"))
                        })?;
                    }
                    "scanner.remote-log.prefetch-num" => {
                        config.scanner_remote_log_prefetch_num =
                            value.parse::<usize>().map_err(|e| {
                                FlussError::new_err(format!(
                                    "Invalid value '{value}' for '{key}': {e}"
                                ))
                            })?;
                    }
                    "remote-file.download-thread-num" => {
                        config.remote_file_download_thread_num =
                            value.parse::<usize>().map_err(|e| {
                                FlussError::new_err(format!(
                                    "Invalid value '{value}' for '{key}': {e}"
                                ))
                            })?;
                    }
                    "scanner.log.max-poll-records" => {
                        config.scanner_log_max_poll_records =
                            value.parse::<usize>().map_err(|e| {
                                FlussError::new_err(format!(
                                    "Invalid value '{value}' for '{key}': {e}"
                                ))
                            })?;
                    }
                    _ => {
                        return Err(FlussError::new_err(format!("Unknown property: {key}")));
                    }
                }
            }
        }

        Ok(Self { inner: config })
    }

    /// Get the bootstrap servers
    #[getter]
    fn bootstrap_servers(&self) -> String {
        self.inner.bootstrap_servers.clone()
    }

    /// Set the bootstrap servers
    #[setter]
    fn set_bootstrap_servers(&mut self, server: String) {
        self.inner.bootstrap_servers = server;
    }

    /// Get the writer request max size
    #[getter]
    fn writer_request_max_size(&self) -> i32 {
        self.inner.writer_request_max_size
    }

    /// Set the writer request max size
    #[setter]
    fn set_writer_request_max_size(&mut self, size: i32) {
        self.inner.writer_request_max_size = size;
    }

    /// Get the writer acks
    #[getter]
    fn writer_acks(&self) -> String {
        self.inner.writer_acks.clone()
    }

    /// Set the writer acks
    #[setter]
    fn set_writer_acks(&mut self, acks: String) {
        self.inner.writer_acks = acks;
    }

    /// Get the writer retries
    #[getter]
    fn writer_retries(&self) -> i32 {
        self.inner.writer_retries
    }

    /// Set the writer retries
    #[setter]
    fn set_writer_retries(&mut self, retries: i32) {
        self.inner.writer_retries = retries;
    }

    /// Get the writer batch size
    #[getter]
    fn writer_batch_size(&self) -> i32 {
        self.inner.writer_batch_size
    }

    /// Set the writer batch size
    #[setter]
    fn set_writer_batch_size(&mut self, size: i32) {
        self.inner.writer_batch_size = size;
    }

    /// Get the scanner remote log prefetch num
    #[getter]
    fn scanner_remote_log_prefetch_num(&self) -> usize {
        self.inner.scanner_remote_log_prefetch_num
    }

    /// Set the scanner remote log prefetch num
    #[setter]
    fn set_scanner_remote_log_prefetch_num(&mut self, num: usize) {
        self.inner.scanner_remote_log_prefetch_num = num;
    }

    /// Get the remote file download thread num
    #[getter]
    fn remote_file_download_thread_num(&self) -> usize {
        self.inner.remote_file_download_thread_num
    }

    /// Set the remote file download thread num
    #[setter]
    fn set_remote_file_download_thread_num(&mut self, num: usize) {
        self.inner.remote_file_download_thread_num = num;
    }

    /// Get the scanner log max poll records
    #[getter]
    fn scanner_log_max_poll_records(&self) -> usize {
        self.inner.scanner_log_max_poll_records
    }

    /// Set the scanner log max poll records
    #[setter]
    fn set_scanner_log_max_poll_records(&mut self, num: usize) {
        self.inner.scanner_log_max_poll_records = num;
    }

    /// Get the writer batch timeout in milliseconds
    #[getter]
    fn writer_batch_timeout_ms(&self) -> i64 {
        self.inner.writer_batch_timeout_ms
    }

    /// Set the writer batch timeout in milliseconds
    #[setter]
    fn set_writer_batch_timeout_ms(&mut self, timeout: i64) {
        self.inner.writer_batch_timeout_ms = timeout;
    }
}

impl Config {
    pub fn get_core_config(&self) -> fcore::config::Config {
        self.inner.clone()
    }
}
