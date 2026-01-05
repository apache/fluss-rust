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

pub use crate::rpc::RpcError;
pub use crate::rpc::{ApiError, FlussError};

use arrow_schema::ArrowError;
use snafu::Snafu;
use std::{io, result};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(
        whatever,
        display("Fluss hitting unexpected error {}: {:?}", message, source)
    )]
    UnexpectedError {
        message: String,
        /// see https://github.com/shepmaster/snafu/issues/446
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync + 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting unexpected io error {}: {:?}", message, source)
    )]
    IoUnexpectedError { message: String, source: io::Error },

    #[snafu(
        visibility(pub(crate)),
        display(
            "Fluss hitting remote storage unexpected error {}: {:?}",
            message,
            source
        )
    )]
    RemoteStorageUnexpectedError {
        message: String,
        source: opendal::Error,
    },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting invalid table error {}.", message)
    )]
    InvalidTableError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting json serde error {}.", message)
    )]
    JsonSerdeError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting unexpected rpc error {}: {:?}", message, source)
    )]
    RpcError { message: String, source: RpcError },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting row convert error {}.", message)
    )]
    RowConvertError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting Arrow error {}: {:?}.", message, source)
    )]
    ArrowError { message: String, source: ArrowError },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting illegal argument error {}.", message)
    )]
    IllegalArgument { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting IO not supported error {}.", message)
    )]
    IoUnsupported { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting wakeup error {}.", message)
    )]
    WakeupError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting leader not available error {}.", message)
    )]
    LeaderNotAvailable { message: String },

    #[snafu(visibility(pub(crate)), display("Fluss API Error: {}.", api_error))]
    FlussAPIError { api_error: ApiError },
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Error::ArrowError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<RpcError> for Error {
    fn from(value: RpcError) -> Self {
        Error::RpcError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IoUnexpectedError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(value: opendal::Error) -> Self {
        Error::RemoteStorageUnexpectedError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<ApiError> for Error {
    fn from(value: ApiError) -> Self {
        Error::FlussAPIError { api_error: value }
    }
}
