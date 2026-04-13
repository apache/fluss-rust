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

//! Async NIF helpers — spawn on tokio, send `{ref, result}` back
//! as a BEAM message instead of blocking dirty schedulers.

use crate::RUNTIME;
use crate::atoms;
use rustler::env::OwnedEnv;
use rustler::{Encoder, Env, Term};
use std::future::Future;

/// Sends `{ref, :ok}` or `{ref, {:error, reason}}` on completion.
pub fn spawn_task<'a, F, E>(env: Env<'a>, future: F) -> Term<'a>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);

    RUNTIME.spawn(async move {
        let result = future.await;
        let _ = task_env.send_and_clear(&pid, |env| {
            let r = saved_ref.load(env);
            match result {
                Ok(()) => (r, atoms::ok()).encode(env),
                Err(e) => (r, (atoms::error(), e.to_string())).encode(env),
            }
        });
    });

    ref_term
}

/// Sends `{ref, {:ok, value}}` or `{ref, {:error, reason}}` on completion.
pub fn spawn_task_with_result<'a, F, T, E>(env: Env<'a>, future: F) -> Term<'a>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Encoder + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);

    RUNTIME.spawn(async move {
        let result = future.await;
        let _ = task_env.send_and_clear(&pid, |env| {
            let r = saved_ref.load(env);
            match result {
                Ok(val) => (r, (atoms::ok(), val)).encode(env),
                Err(e) => (r, (atoms::error(), e.to_string())).encode(env),
            }
        });
    });

    ref_term
}

/// Sends `{ref, {:error, reason}}` immediately (no async work).
pub fn send_error<'a>(env: Env<'a>, msg: &str) -> Term<'a> {
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);
    let msg = msg.to_string();

    let _ = task_env.send_and_clear(&pid, |env| {
        let r = saved_ref.load(env);
        (r, (atoms::error(), msg)).encode(env)
    });

    ref_term
}
