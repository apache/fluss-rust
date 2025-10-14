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

use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use bytes::{Buf, BufMut};

mod create_database;
mod create_table;
mod database_exists;
mod drop_database;
mod drop_table;
mod fetch;
mod get_database_info;
mod get_latest_lake_snapshot;
mod get_table;
mod header;
mod list_databases;
mod list_offsets;
mod list_tables;
mod produce_log;
mod table_exists;
mod update_metadata;

pub use create_database::*;
pub use create_table::*;
pub use database_exists::*;
pub use drop_database::*;
pub use drop_table::*;
pub use fetch::*;
pub use get_database_info::*;
pub use get_latest_lake_snapshot::*;
pub use get_table::*;
pub use header::*;
pub use list_databases::*;
pub use list_offsets::*;
pub use list_tables::*;
pub use produce_log::*;
pub use table_exists::*;
pub use update_metadata::*;

pub trait RequestBody {
    type ResponseBody;

    const API_KEY: ApiKey;

    const REQUEST_VERSION: ApiVersion;
}

impl<T: RequestBody> RequestBody for &T {
    type ResponseBody = T::ResponseBody;

    const API_KEY: ApiKey = T::API_KEY;

    const REQUEST_VERSION: ApiVersion = T::REQUEST_VERSION;
}

pub trait WriteVersionedType<W>: Sized
where
    W: BufMut,
{
    fn write_versioned(&self, writer: &mut W, version: ApiVersion) -> Result<(), WriteError>;
}

pub trait ReadVersionedType<R>: Sized
where
    R: Buf,
{
    fn read_versioned(reader: &mut R, version: ApiVersion) -> Result<Self, ReadError>;
}

#[macro_export]
macro_rules! impl_write_version_type {
    ($type:ty) => {
        impl<W> WriteVersionedType<W> for $type
        where
            W: BufMut,
        {
            fn write_versioned(
                &self,
                writer: &mut W,
                _version: ApiVersion,
            ) -> Result<(), WriteError> {
                Ok(self.inner_request.encode(writer).unwrap())
            }
        }
    };
}

#[macro_export]
macro_rules! impl_read_version_type {
    ($type:ty) => {
        impl<R> ReadVersionedType<R> for $type
        where
            R: Buf,
        {
            fn read_versioned(reader: &mut R, _version: ApiVersion) -> Result<Self, ReadError> {
                Ok(<$type>::decode(reader).unwrap())
            }
        }
    };
}
