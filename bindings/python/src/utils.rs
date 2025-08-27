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
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;
use arrow_pyarrow::ToPyArrow;
use crate::*;

/// Utilities for schema conversion between PyArrow, Arrow, and Fluss
pub struct Utils;

impl Utils {
    /// Convert PyArrow schema to Rust Arrow schema
    pub fn pyarrow_to_arrow_schema(py_schema: &PyObject) -> PyResult<SchemaRef> {
        Python::with_gil(|py| {
            let schema_bound = py_schema.bind(py);
            
            let schema: ArrowSchema = arrow_pyarrow::FromPyArrow::from_pyarrow_bound(&schema_bound)
                .map_err(|e| FlussError::new_err(format!("Failed to convert PyArrow schema: {}", e)))?;
            Ok(Arc::new(schema))
        })
    }

    /// Convert Arrow DataType to Fluss DataType
    pub fn arrow_type_to_fluss_type(arrow_type: &arrow::datatypes::DataType) -> PyResult<fcore::metadata::DataType> {
        use arrow::datatypes::DataType as ArrowDataType;
        use fcore::metadata::DataTypes;

        let fluss_type = match arrow_type {
            ArrowDataType::Boolean => DataTypes::boolean(),
            ArrowDataType::Int8 => DataTypes::tinyint(),
            ArrowDataType::Int16 => DataTypes::smallint(),
            ArrowDataType::Int32 => DataTypes::int(),
            ArrowDataType::Int64 => DataTypes::bigint(),
            ArrowDataType::UInt8 => DataTypes::tinyint(),
            ArrowDataType::UInt16 => DataTypes::smallint(),
            ArrowDataType::UInt32 => DataTypes::int(),
            ArrowDataType::UInt64 => DataTypes::bigint(),
            ArrowDataType::Float32 => DataTypes::float(),
            ArrowDataType::Float64 => DataTypes::double(),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataTypes::string(),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataTypes::bytes(),
            ArrowDataType::Date32 => DataTypes::date(),
            ArrowDataType::Date64 => DataTypes::date(),
            ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => DataTypes::time(),
            ArrowDataType::Timestamp(_, _) => DataTypes::timestamp(),
            ArrowDataType::Decimal128(precision, scale) => DataTypes::decimal(*precision as u32, *scale as u32),
            _ => {
                return Err(FlussError::new_err(format!(
                    "Unsupported Arrow data type: {:?}", arrow_type
                )));
            }
        };

        Ok(fluss_type)
    }

    /// Convert Fluss DataType to string representation
    pub fn datatype_to_string(data_type: &fcore::metadata::DataType) -> String {
        match data_type {
            fcore::metadata::DataType::Boolean(_) => "boolean".to_string(),
            fcore::metadata::DataType::TinyInt(_) => "tinyint".to_string(),
            fcore::metadata::DataType::SmallInt(_) => "smallint".to_string(),
            fcore::metadata::DataType::Int(_) => "int".to_string(),
            fcore::metadata::DataType::BigInt(_) => "bigint".to_string(),
            fcore::metadata::DataType::Float(_) => "float".to_string(),
            fcore::metadata::DataType::Double(_) => "double".to_string(),
            fcore::metadata::DataType::String(_) => "string".to_string(),
            fcore::metadata::DataType::Bytes(_) => "bytes".to_string(),
            fcore::metadata::DataType::Date(_) => "date".to_string(),
            fcore::metadata::DataType::Time(t) => {
                if t.precision() == 0 {
                    "time".to_string()
                } else {
                    format!("time({})", t.precision())
                }
            },
            fcore::metadata::DataType::Timestamp(t) => {
                if t.precision() == 6 {
                    "timestamp".to_string()
                } else {
                    format!("timestamp({})", t.precision())
                }
            },
            fcore::metadata::DataType::TimestampLTz(t) => {
                if t.precision() == 6 {
                    "timestamp_ltz".to_string()
                } else {
                    format!("timestamp_ltz({})", t.precision())
                }
            },
            fcore::metadata::DataType::Char(c) => format!("char({})", c.length()),
            fcore::metadata::DataType::Decimal(d) => format!("decimal({},{})", d.precision(), d.scale()),
            fcore::metadata::DataType::Binary(b) => format!("binary({})", b.length()),
            fcore::metadata::DataType::Array(arr) => format!("array<{}>", Utils::datatype_to_string(arr.get_element_type())),
            fcore::metadata::DataType::Map(map) => format!("map<{},{}>", 
                                        Utils::datatype_to_string(map.key_type()), 
                                        Utils::datatype_to_string(map.value_type())),
            fcore::metadata::DataType::Row(row) => {
                let fields: Vec<String> = row.fields().iter()
                    .map(|field| format!("{}: {}", field.name(), Utils::datatype_to_string(field.data_type())))
                    .collect();
                format!("row<{}>", fields.join(", "))
            },
        }
    }

    /// Parse log format string to LogFormat enum
    pub fn parse_log_format(format_str: &str) -> PyResult<fcore::metadata::LogFormat> {
        fcore::metadata::LogFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid log format '{}': {}", format_str, e)))
    }

    /// Parse kv format string to KvFormat enum
    pub fn parse_kv_format(format_str: &str) -> PyResult<fcore::metadata::KvFormat> {
        fcore::metadata::KvFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid kv format '{}': {}", format_str, e)))
    }

    /// Convert ScanRecords to Arrow RecordBatch
    pub fn convert_scan_records_to_arrow(
        _scan_records: fcore::record::ScanRecords,
    ) -> Vec<Arc<arrow::record_batch::RecordBatch>> {
        let mut result = Vec::new();
        for(_, records) in _scan_records.into_records() {
            for record in records {
                let columnar_row = record.row();
                let row_id = columnar_row.get_row_id();
                if row_id == 0 {
                    let record_batch = columnar_row.get_record_batch();
                    result.push(record_batch.clone());
                }
            }
        }
        result
    }
    
    /// Combine multiple Arrow batches into a single Table
    pub fn combine_batches_to_table(py: Python, batches: Vec<Arc<arrow::record_batch::RecordBatch>>) -> PyResult<PyObject> {
        if batches.is_empty() {
            return Err(FlussError::new_err("No batches to combine"));
        }
        
        // Convert Rust Arrow RecordBatch to PyObject
        let py_batches: Result<Vec<PyObject>, _> = batches.iter()
            .map(|batch| {
                batch.as_ref().to_pyarrow(py)
                    .map_err(|e| FlussError::new_err(format!("Failed to convert RecordBatch to PyObject: {}", e)))
            })
            .collect();
        
        let py_batches = py_batches?;
        
        let pyarrow = py.import("pyarrow")?;
        
        // Use pyarrow.Table.from_batches to combine batches
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (py_batches,))?;
        
        Ok(table.into())
    }
}
