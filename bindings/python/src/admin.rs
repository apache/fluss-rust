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
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Administrative client for managing Fluss tables
#[pyclass]
pub struct FlussAdmin {
    __admin: Arc<fcore::client::FlussAdmin>,
}

#[pymethods]
impl FlussAdmin {
    /// Create a table with the given schema
    #[pyo3(signature = (table_path, table_descriptor, ignore_if_exists=None))]
    pub fn create_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ignore = ignore_if_exists.unwrap_or(false);

        let core_table_path = table_path.to_core().clone();
        let core_descriptor = table_descriptor.to_core().clone();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            admin
                .create_table(&core_table_path, &core_descriptor, ignore)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// Get table information
    pub fn get_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core().clone();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let core_table_info = admin
                .get_table(&core_table_path)
                .await
                .map_err(|e| FlussError::new_err(format!("Failed to get table: {e}")))?;

            Python::attach(|py| {
                let table_info = TableInfo::from_core(core_table_info);
                Py::new(py, table_info)
            })
        })
    }

    /// Get the latest lake snapshot for a table
    pub fn get_latest_lake_snapshot<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core().clone();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            let core_lake_snapshot = admin
                .get_latest_lake_snapshot(&core_table_path)
                .await
                .map_err(|e| FlussError::new_err(format!("Failed to get lake snapshot: {e}")))?;

            Python::attach(|py| {
                let lake_snapshot = LakeSnapshot::from_core(core_lake_snapshot);
                Py::new(py, lake_snapshot)
            })
        })
    }

    /// Drop a table
    #[pyo3(signature = (table_path, ignore_if_not_exists=false))]
    pub fn drop_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        ignore_if_not_exists: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            admin
                .drop_table(&core_table_path, ignore_if_not_exists)
                .await
                .map_err(|e| FlussError::new_err(format!("Failed to drop table: {e}")))?;

            Python::attach(|py| Ok(py.None()))
        })
    }

    /// List offsets for buckets.
    ///
    /// Args:
    ///     table_path: Path to the table
    ///     bucket_ids: List of bucket IDs to query
    ///     offset_type: Type of offset to retrieve:
    ///         - "earliest" or OffsetType.EARLIEST: Start of the log
    ///         - "latest" or OffsetType.LATEST: End of the log
    ///         - "timestamp" or OffsetType.TIMESTAMP: Offset at given timestamp (requires timestamp arg)
    ///     timestamp: Required when offset_type is "timestamp", ignored otherwise
    ///
    /// Returns:
    ///     dict[int, int]: Mapping of bucket_id -> offset
    ///
    /// Example:
    ///     >>> offsets = await admin.list_offsets(table_path, [0, 1], "latest")
    ///     >>> print(offsets)  # {0: 100, 1: 150}
    #[pyo3(signature = (table_path, bucket_ids, offset_type, timestamp=None))]
    pub fn list_offsets<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        bucket_ids: Vec<i32>,
        offset_type: &str,
        timestamp: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use fcore::rpc::message::OffsetSpec;

        // Validate bucket IDs
        for &bucket_id in &bucket_ids {
            if bucket_id < 0 {
                return Err(FlussError::new_err(format!(
                    "Invalid bucket_id: {}. Bucket IDs must be non-negative",
                    bucket_id
                )));
            }
        }

        let core_table_path = table_path.to_core();
        let admin = self.__admin.clone();

        // Parse offset_type (case-insensitive, no allocation)
        let offset_spec = match offset_type {
            s if s.eq_ignore_ascii_case("earliest") => OffsetSpec::Earliest,
            s if s.eq_ignore_ascii_case("latest") => OffsetSpec::Latest,
            s if s.eq_ignore_ascii_case("timestamp") => {
                let ts = timestamp.ok_or_else(|| {
                    FlussError::new_err(
                        "timestamp must be provided when offset_type='timestamp'".to_string(),
                    )
                })?;
                OffsetSpec::Timestamp(ts)
            }
            _ => {
                return Err(FlussError::new_err(format!(
                    "Invalid offset_type: '{}'. Must be 'earliest', 'latest', or 'timestamp'",
                    offset_type
                )))
            }
        };

        future_into_py(py, async move {
            let offsets = admin
                .list_offsets(&core_table_path, &bucket_ids, offset_spec)
                .await
                .map_err(|e| FlussError::new_err(format!("Failed to list offsets: {e}")))?;

            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                for (bucket_id, offset) in offsets {
                    dict.set_item(bucket_id, offset)?;
                }
                Ok(dict.unbind())
            })
        })
    }

    fn __repr__(&self) -> String {
        "FlussAdmin()".to_string()
    }
}

impl FlussAdmin {
    // Internal method to create FlussAdmin from core admin
    pub fn from_core(admin: fcore::client::FlussAdmin) -> Self {
        Self {
            __admin: Arc::new(admin),
        }
    }
}
