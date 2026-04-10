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
use fluss::client::WriteResultFuture;
use rustler::{Atom, ResourceArc};
use std::sync::Mutex;

pub struct WriteHandleResource(Mutex<Option<WriteResultFuture>>);

impl std::panic::RefUnwindSafe for WriteHandleResource {}

#[rustler::resource_impl]
impl rustler::Resource for WriteHandleResource {}

impl WriteHandleResource {
    pub fn new(future: WriteResultFuture) -> Self {
        Self(Mutex::new(Some(future)))
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn write_handle_wait(handle: ResourceArc<WriteHandleResource>) -> Result<Atom, rustler::Error> {
    let future = handle
        .0
        .lock()
        .map_err(|e| to_nif_err(format!("lock poisoned: {e}")))?
        .take()
        .ok_or_else(|| to_nif_err("WriteHandle already consumed"))?;

    RUNTIME.block_on(future).map_err(to_nif_err)?;
    Ok(atoms::ok())
}
