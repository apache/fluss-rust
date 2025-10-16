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
                        config.bootstrap_server = Some(value);
                    }
                    "request.max.size" => {
                        if let Ok(size) = value.parse::<i32>() {
                            config.request_max_size = size;
                        }
                    }
                    "writer.acks" => {
                        config.writer_acks = value;
                    }
                    "writer.retries" => {
                        if let Ok(retries) = value.parse::<i32>() {
                            config.writer_retries = retries;
                        }
                    }
                    "writer.batch.size" => {
                        if let Ok(size) = value.parse::<i32>() {
                            config.writer_batch_size = size;
                        }
                    }
                    _ => {
                        return Err(FlussError::new_err(format!("Unknown property: {key}")));
                    }
                }
            }
        }

        Ok(Self { inner: config })
    }

    /// Get the bootstrap server
    #[getter]
    fn bootstrap_server(&self) -> Option<String> {
        self.inner.bootstrap_server.clone()
    }

    /// Set the bootstrap server
    #[setter]
    fn set_bootstrap_server(&mut self, server: String) {
        self.inner.bootstrap_server = Some(server);
    }

    /// Get the request max size
    #[getter]
    fn request_max_size(&self) -> i32 {
        self.inner.request_max_size
    }

    /// Set the request max size
    #[setter]
    fn set_request_max_size(&mut self, size: i32) {
        self.inner.request_max_size = size;
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
}

impl Config {
    pub fn get_core_config(&self) -> fcore::config::Config {
        self.inner.clone()
    }
}
