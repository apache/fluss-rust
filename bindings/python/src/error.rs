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

use pyo3::exceptions::PyException;
use pyo3::prelude::*;

/// Fluss errors
#[pyclass(extends=PyException)]
#[derive(Debug, Clone)]
pub struct FlussError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub error_code: i32,
}

#[pymethods]
impl FlussError {
    #[new]
    #[pyo3(signature = (message, error_code=0))]
    fn new(message: String, error_code: i32) -> Self {
        Self {
            message,
            error_code,
        }
    }

    fn __str__(&self) -> String {
        if self.error_code != 0 {
            format!("FlussError(code={}): {}", self.error_code, self.message)
        } else {
            format!("FlussError: {}", self.message)
        }
    }
}

impl FlussError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<FlussError, _>((message.to_string(), 0i32))
    }

    /// Create a PyErr from a core Error, extracting the API error code if available.
    pub fn from_core_error(error: &fluss::error::Error) -> PyErr {
        if let fluss::error::Error::FlussAPIError { api_error } = error {
            PyErr::new::<FlussError, _>((api_error.message.clone(), api_error.code))
        } else {
            PyErr::new::<FlussError, _>((error.to_string(), 0i32))
        }
    }
}
