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

use crate::rpc::api_key::ApiKey::Unknown;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ApiKey {
    CreateDatabase,
    DropDatabase,
    ListDatabases,
    DatabaseExists,
    CreateTable,
    DropTable,
    GetTable,
    ListTables,
    TableExists,
    MetaData,
    ProduceLog,
    FetchLog,
    ListOffsets,
    GetDatabaseInfo,
    GetLatestLakeSnapshot,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        match key {
            1001 => ApiKey::CreateDatabase,
            1002 => ApiKey::DropDatabase,
            1003 => ApiKey::ListDatabases,
            1004 => ApiKey::DatabaseExists,
            1005 => ApiKey::CreateTable,
            1006 => ApiKey::DropTable,
            1007 => ApiKey::GetTable,
            1008 => ApiKey::ListTables,
            1010 => ApiKey::TableExists,
            1012 => ApiKey::MetaData,
            1014 => ApiKey::ProduceLog,
            1015 => ApiKey::FetchLog,
            1021 => ApiKey::ListOffsets,
            1032 => ApiKey::GetLatestLakeSnapshot,
            1035 => ApiKey::GetDatabaseInfo,
            _ => Unknown(key),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::CreateDatabase => 1001,
            ApiKey::DropDatabase => 1002,
            ApiKey::ListDatabases => 1003,
            ApiKey::DatabaseExists => 1004,
            ApiKey::CreateTable => 1005,
            ApiKey::DropTable => 1006,
            ApiKey::GetTable => 1007,
            ApiKey::ListTables => 1008,
            ApiKey::TableExists => 1010,
            ApiKey::MetaData => 1012,
            ApiKey::ProduceLog => 1014,
            ApiKey::FetchLog => 1015,
            ApiKey::ListOffsets => 1021,
            ApiKey::GetLatestLakeSnapshot => 1032,
            ApiKey::GetDatabaseInfo => 1035,
            Unknown(x) => x,
        }
    }
}
