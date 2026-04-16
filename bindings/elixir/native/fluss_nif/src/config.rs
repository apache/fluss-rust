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

use fluss::config::Config;
use rustler::NifStruct;

/// Decoded from `%Fluss.Config{}` Elixir struct.
#[derive(NifStruct)]
#[module = "Fluss.Config"]
pub struct NifConfig {
    pub bootstrap_servers: String,
    pub writer_batch_size: Option<i32>,
    pub writer_batch_timeout_ms: Option<i64>,
}

impl NifConfig {
    pub fn into_core(self) -> Config {
        let mut config = Config {
            bootstrap_servers: self.bootstrap_servers,
            ..Config::default()
        };
        if let Some(size) = self.writer_batch_size {
            config.writer_batch_size = size;
        }
        if let Some(ms) = self.writer_batch_timeout_ms {
            config.writer_batch_timeout_ms = ms;
        }
        config
    }
}
