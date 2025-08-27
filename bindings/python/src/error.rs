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

use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyConnectionError, PyValueError, PyIOError};
use fluss::error::Error as FlussRustError;
use std::fmt;

/// Custom exception for Fluss errors
#[pyclass(extends=PyException)]
#[derive(Debug, Clone)]
pub struct FlussError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub error_code: Option<i32>,
}

#[pymethods]
impl FlussError {
    #[new]
    #[pyo3(signature = (message, error_code = None))]
    fn new(message: String, error_code: Option<i32>) -> Self {
        FlussError {
            message,
            error_code,
        }
    }

    fn __str__(&self) -> String {
        if let Some(code) = self.error_code {
            format!("FlussError({}): {}", code, self.message)
        } else {
            format!("FlussError: {}", self.message)
        }
    }

    fn __repr__(&self) -> String {
        if let Some(code) = self.error_code {
            format!("FlussError(message='{}', error_code={})", self.message, code)
        } else {
            format!("FlussError(message='{}')", self.message)
        }
    }
}

impl fmt::Display for FlussError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.__str__())
    }
}

impl std::error::Error for FlussError {}

// Convert from Rust fluss error to Python FlussError
impl From<FlussRustError> for FlussError {
    fn from(error: FlussRustError) -> Self {
        FlussError {
            message: error.to_string(),
            error_code: None,
        }
    }
}

// Convert FlussError to PyErr for use with PyO3
impl From<FlussError> for PyErr {
    fn from(error: FlussError) -> Self {
        PyErr::new::<FlussError, _>(error.message.clone())
    }
}

// Helper function to convert Rust fluss error to appropriate Python exception
pub fn fluss_error_to_pyerr(error: FlussRustError) -> PyErr {
    match error {
        FlussRustError::Io(_) => PyIOError::new_err(error.to_string()),
        FlussRustError::IllegalArgument(_) => PyValueError::new_err(error.to_string()),
        FlussRustError::RpcError(_) => PyConnectionError::new_err(error.to_string()),
        FlussRustError::InvalidTableError(_) => PyValueError::new_err(error.to_string()),
        _ => FlussError::from(error).into(),
    }
}

// Helper functions for creating FlussError
impl FlussError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<FlussError, _>(message.to_string())
    }

    pub fn new_with_code(message: impl ToString, code: i32) -> Self {
        FlussError {
            message: message.to_string(),
            error_code: Some(code),
        }
    }

    pub fn new_with_code_err(message: impl ToString, code: i32) -> PyErr {
        let error = FlussError::new_with_code(message, code);
        PyErr::new::<FlussError, _>(error.message)
    }
}
