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

use crate::RUNTIME;
use crate::atoms::{self, to_nif_err};
use crate::row_convert;
use crate::table::TableResource;
use crate::write_handle::WriteHandleResource;
use fluss::client::AppendWriter;
use fluss::metadata::Column;
use rustler::{Atom, Env, ResourceArc, Term};

pub struct AppendWriterResource {
    pub writer: AppendWriter,
    pub columns: Vec<Column>,
}

impl std::panic::RefUnwindSafe for AppendWriterResource {}

#[rustler::resource_impl]
impl rustler::Resource for AppendWriterResource {}

#[rustler::nif]
fn append_writer_new(
    table: ResourceArc<TableResource>,
) -> Result<ResourceArc<AppendWriterResource>, rustler::Error> {
    // WriterClient::new() calls tokio::spawn internally.
    let _guard = RUNTIME.enter();
    let (writer, columns) = table.with_table(|t| {
        let writer = t
            .new_append()
            .map_err(to_nif_err)?
            .create_writer()
            .map_err(to_nif_err)?;
        Ok((writer, t.get_table_info().schema.columns().to_vec()))
    })?;
    Ok(ResourceArc::new(AppendWriterResource { writer, columns }))
}

#[rustler::nif]
fn append_writer_append<'a>(
    env: Env<'a>,
    writer: ResourceArc<AppendWriterResource>,
    values: Term<'a>,
) -> Result<ResourceArc<WriteHandleResource>, rustler::Error> {
    let row = row_convert::term_to_row(env, values, &writer.columns).map_err(to_nif_err)?;
    let future = writer.writer.append(&row).map_err(to_nif_err)?;
    Ok(ResourceArc::new(WriteHandleResource::new(future)))
}

#[rustler::nif(schedule = "DirtyIo")]
fn append_writer_flush(writer: ResourceArc<AppendWriterResource>) -> Result<Atom, rustler::Error> {
    RUNTIME
        .block_on(writer.writer.flush())
        .map_err(to_nif_err)?;
    Ok(atoms::ok())
}
