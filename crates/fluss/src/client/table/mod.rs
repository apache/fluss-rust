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
use crate::metadata::{TableInfo, TablePath};
use std::sync::Arc;

pub const EARLIEST_OFFSET: i64 = -2;

mod append;
mod lookup;

mod log_fetch_buffer;
mod remote_log;
mod scanner;
mod writer;

pub use append::{AppendWriter, TableAppend};
pub use lookup::{LookupResult, Lookuper, TableLookup};
pub use scanner::{LogScanner, RecordBatchLogScanner, TableScan};

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
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> TableScan<'_> {
        TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone())
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
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}
