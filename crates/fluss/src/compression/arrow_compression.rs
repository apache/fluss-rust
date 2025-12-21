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

use crate::error::{Error, Result};
use arrow::ipc::CompressionType;
use std::collections::HashMap;

pub const TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL: &str = "table.log.arrow.compression.zstd.level";
pub const TABLE_LOG_ARROW_COMPRESSION_TYPE: &str = "table.log.arrow.compression.type";
pub const DEFAULT_NON_ZSTD_COMPRESSION_LEVEL: i32 = -1;
pub const DEFAULT_ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Clone, PartialEq)]
pub enum ArrowCompressionType {
    None,
    Lz4Frame,
    Zstd,
}

impl ArrowCompressionType {
    fn from_conf(properties: &HashMap<String, String>) -> Result<Self> {
        match properties
            .get(TABLE_LOG_ARROW_COMPRESSION_TYPE)
            .map(|s| s.as_str())
        {
            Some("NONE") => Ok(Self::None),
            Some("LZ4_FRAME") => Ok(Self::Lz4Frame),
            Some("ZSTD") => Ok(Self::Zstd),
            Some(other) => Err(Error::IllegalArgument {
                message: format!("Unsupported compression type: {other}"),
            }),
            None => Ok(Self::Zstd),
        }
    }
}

#[derive(Clone)]
pub struct ArrowCompressionInfo {
    pub compression_type: ArrowCompressionType,
    pub compression_level: i32,
}

impl ArrowCompressionInfo {
    pub fn from_conf(properties: &HashMap<String, String>) -> Result<Self> {
        let compression_type = ArrowCompressionType::from_conf(properties)?;

        if compression_type != ArrowCompressionType::Zstd {
            return Ok(Self {
                compression_type,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            });
        }

        match properties
            .get(TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL)
            .map(|s| s.as_str().parse::<i32>())
        {
            Some(Ok(level)) if !(1..=22).contains(&level) => Err(Error::IllegalArgument {
                message: format!(
                    "Invalid ZSTD compression level: {}. Expected a value between 1 and 22.",
                    level
                ),
            }),
            Some(Err(e)) => Err(Error::IllegalArgument {
                message: format!(
                    "Invalid ZSTD compression level. Expected a value between 1 and 22. {}",
                    e
                ),
            }),

            Some(Ok(level)) => Ok(Self {
                compression_type,
                compression_level: level - 1,
            }),
            None => Ok(Self {
                compression_type,
                compression_level: DEFAULT_ZSTD_COMPRESSION_LEVEL,
            }),
        }
    }

    pub fn get_compression_type(&self) -> Option<CompressionType> {
        match self.compression_type {
            ArrowCompressionType::Zstd => Some(CompressionType::ZSTD),
            ArrowCompressionType::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            ArrowCompressionType::None => None,
        }
    }
}
