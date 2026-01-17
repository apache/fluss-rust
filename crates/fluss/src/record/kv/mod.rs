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

//! Key-Value record and batch implementations.

mod kv_record;
mod kv_record_batch;
mod kv_record_batch_builder;
mod kv_record_read_context;
mod read_context;

pub use kv_record::{KvRecord, LENGTH_LENGTH as KV_RECORD_LENGTH_LENGTH};
pub use kv_record_batch::*;
pub use kv_record_batch_builder::*;
pub use kv_record_read_context::{KvRecordReadContext, SchemaGetter};
pub use read_context::ReadContext;

/// Current KV magic value
pub const CURRENT_KV_MAGIC_VALUE: u8 = 0;

/// No writer ID constant
pub const NO_WRITER_ID: i64 = -1;

/// No batch sequence constant
pub const NO_BATCH_SEQUENCE: i32 = -1;

/// Test utilities for KV record reading.
#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::metadata::{DataType, KvFormat};
    use crate::row::{RowDecoder, RowDecoderFactory};
    use std::io;
    use std::sync::Arc;

    /// Simple test-only ReadContext that creates decoders directly from data types.
    ///
    /// This bypasses the production Schema/SchemaGetter machinery for simpler tests.
    pub struct TestReadContext {
        kv_format: KvFormat,
        data_types: Vec<DataType>,
    }

    impl TestReadContext {
        /// Create a new test context with the given format and data types.
        pub fn new(kv_format: KvFormat, data_types: Vec<DataType>) -> Self {
            Self {
                kv_format,
                data_types,
            }
        }

        /// Create a test context for COMPACTED format (most common case).
        pub fn compacted(data_types: Vec<DataType>) -> Self {
            Self::new(KvFormat::COMPACTED, data_types)
        }
    }

    impl ReadContext for TestReadContext {
        fn get_row_decoder(&self, _schema_id: i16) -> io::Result<Arc<dyn RowDecoder>> {
            // Directly create decoder from data types - no Schema needed!
            RowDecoderFactory::create(self.kv_format.clone(), self.data_types.clone())
        }
    }
}
