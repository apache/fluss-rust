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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::KvFormat;
use std::fmt::Display;

pub enum WriteFormat {
    ArrowLog,
    IndexedLog,
    CompactedLog,
    IndexedKv,
    CompactedKv,
}

impl WriteFormat {
    pub fn is_log(&self) -> bool {
        match self {
            WriteFormat::ArrowLog => true,
            WriteFormat::IndexedLog => true,
            WriteFormat::CompactedLog => true,
            WriteFormat::IndexedKv => false,
            WriteFormat::CompactedKv => false,
        }
    }

    pub fn is_kv(&self) -> bool {
        !self.is_log()
    }

    pub fn to_kv_format(&self) -> Result<KvFormat> {
        match self {
            WriteFormat::IndexedKv => Ok(KvFormat::INDEXED),
            WriteFormat::CompactedKv => Ok(KvFormat::COMPACTED),
            other => Err(IllegalArgument {
                message: format!("WriteFormat `{}` is not a KvFormat", other),
            }),
        }
    }

    pub fn from_kv_format(kv_format: &KvFormat) -> Result<Self> {
        match kv_format {
            KvFormat::INDEXED => Ok(WriteFormat::IndexedKv),
            KvFormat::COMPACTED => Ok(WriteFormat::CompactedKv),
        }
    }
}

impl Display for WriteFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteFormat::ArrowLog => f.write_str("ArrowLog"),
            WriteFormat::IndexedLog => f.write_str("IndexedLog"),
            WriteFormat::CompactedLog => f.write_str("CompactedLog"),
            WriteFormat::IndexedKv => f.write_str("IndexedKv"),
            WriteFormat::CompactedKv => f.write_str("CompactedKv"),
        }
    }
}
