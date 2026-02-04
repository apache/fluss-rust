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

use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::metadata::{LogFormat, TableInfo, TablePath};
use std::sync::Arc;

pub const EARLIEST_OFFSET: i64 = -2;

mod append;
mod lookup;

mod log_fetch_buffer;
mod partition_getter;
mod remote_log;
mod scanner;
mod upsert;

use crate::error::Error::UnsupportedOperation;
pub use append::{AppendWriter, TableAppend};
pub use lookup::{LookupResult, Lookuper, TableLookup};
pub use remote_log::{
    DEFAULT_SCANNER_REMOTE_LOG_DOWNLOAD_THREADS, DEFAULT_SCANNER_REMOTE_LOG_PREFETCH_NUM,
};
pub use scanner::{LogScanner, RecordBatchLogScanner, TableScan};
pub use upsert::{TableUpsert, UpsertWriter};

#[allow(dead_code)]
pub struct FlussTable<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl<'a> FlussTable<'a> {
    pub fn new(conn: &'a FlussConnection, metadata: Arc<Metadata>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
            metadata,
        }
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn new_append(&self) -> Result<TableAppend> {
        Ok(TableAppend::new(
            self.table_path.clone(),
            Arc::new(self.table_info.clone()),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> Result<TableScan<'_>> {
        let table_path = &self.table_path;
        let table_info = &self.table_info;

        validate_scan_support(table_path, table_info)?;

        Ok(TableScan::new(
            self.conn,
            self.table_info.clone(),
            self.metadata.clone(),
        ))
    }

    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    /// Creates a new `TableLookup` for configuring lookup operations.
    ///
    /// This follows the same pattern as `new_scan()` and `new_append()`,
    /// returning a configuration object that can be used to create a `Lookuper`.
    ///
    /// The table must have a primary key (be a primary key table).
    ///
    /// # Returns
    /// * `Ok(TableLookup)` - A lookup configuration object
    /// * `Err(Error)` - If the table doesn't have a primary key
    ///
    /// # Example
    /// ```ignore
    /// let table = conn.get_table(&table_path).await?;
    /// let lookuper = table.new_lookup()?.create_lookuper()?;
    /// let key = vec![1, 2, 3]; // encoded primary key bytes
    /// if let Some(value) = lookuper.lookup(key).await? {
    ///     println!("Found value: {:?}", value);
    /// }
    /// ```
    pub fn new_lookup(&self) -> Result<TableLookup<'_>> {
        if !self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Lookup is only supported for primary key tables".to_string(),
            });
        }
        Ok(TableLookup::new(
            self.conn,
            self.table_info.clone(),
            self.metadata.clone(),
        ))
    }

    pub fn new_upsert(&self) -> Result<TableUpsert> {
        if !self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Upsert is only supported for primary key tables".to_string(),
            });
        }

        Ok(TableUpsert::new(
            self.table_path.clone(),
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }
}

fn validate_scan_support(table_path: &TablePath, table_info: &TableInfo) -> Result<()> {
    if table_info.schema.primary_key().is_some() {
        return Err(UnsupportedOperation {
            message: format!("Table {table_path} is not a Log Table and doesn't support scan."),
        });
    }

    let log_format = table_info.table_config.get_log_format()?;
    if LogFormat::ARROW != log_format {
        return Err(UnsupportedOperation {
            message: format!(
                "Scan is only supported for ARROW format and table {table_path} uses {log_format} format"
            ),
        });
    }

    Ok(())
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, Schema, TableInfo, TablePath};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_table_info(
        has_primary_key: bool,
        log_format: Option<&str>,
    ) -> (TableInfo, TablePath) {
        let mut schema_builder = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string());

        if has_primary_key {
            schema_builder = schema_builder.primary_key(vec!["id"]);
        }

        let schema = schema_builder.build().unwrap();
        let table_path = TablePath::new("test_db", "test_table");

        let mut properties = HashMap::new();
        if let Some(format) = log_format {
            properties.insert("table.log.format".to_string(), format.to_string());
        }

        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema,
            vec![],
            Arc::from(vec![]),
            1,
            properties,
            HashMap::new(),
            None,
            0,
            0,
        );

        (table_info, table_path)
    }

    #[test]
    fn test_validate_scan_support() {
        // Primary key table
        let (table_info, table_path) = create_test_table_info(true, Some("ARROW"));
        let result = validate_scan_support(&table_path, &table_info);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, UnsupportedOperation { .. }));
        assert!(err.to_string().contains(
            format!("Table {table_path} is not a Log Table and doesn't support scan.").as_str()
        ));

        // Indexed format
        let (table_info, table_path) = create_test_table_info(false, Some("INDEXED"));
        let result = validate_scan_support(&table_path, &table_info);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, UnsupportedOperation { .. }));
        assert!(err.to_string().contains(format!("Scan is only supported for ARROW format and table {table_path} uses INDEXED format").as_str()));

        // Default format
        let (table_info, table_path) = create_test_table_info(false, None);
        let result = validate_scan_support(&table_path, &table_info);
        assert!(result.is_ok());

        // Arrow format
        let (table_info, table_path) = create_test_table_info(false, Some("ARROW"));
        let result = validate_scan_support(&table_path, &table_info);
        assert!(result.is_ok());
    }
}
