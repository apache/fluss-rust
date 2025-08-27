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

use crate::cluster::ServerNode;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::error::RpcError;
use crate::rpc::error::RpcError::ConnectionError;
use crate::rpc::frame::{AsyncMessageRead, AsyncMessageWrite};
use crate::rpc::message::{
    ReadVersionedType, RequestBody, RequestHeader, ResponseHeader, WriteVersionedType,
};
use crate::rpc::transport::Transport;
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufStream, WriteHalf};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;
use tracing::warn;

pub type MessengerTransport = ServerConnectionInner<BufStream<Transport>>;

pub type ServerConnection = Arc<MessengerTransport>;

#[derive(Debug, Default)]
pub struct RpcClient {
    connections: RwLock<HashMap<String, ServerConnection>>,
    client_id: Arc<str>,
    timeout: Option<Duration>,
    max_message_size: usize,
}

impl RpcClient {
    pub fn new() -> Self {
        RpcClient {
            connections: Default::default(),
            client_id: Arc::from(""),
            timeout: None,
            max_message_size: usize::MAX,
        }
    }

    pub async fn get_connection(
        &self,
        server_node: &ServerNode,
    ) -> Result<ServerConnection, RpcError> {
        let server_id = server_node.uid();
        {
            let connections = self.connections.read();
            if let Some(connection) = connections.get(server_id) {
                return Ok(connection.clone());
            }
        }
        let new_server = self.connect(server_node).await?;
        self.connections
            .write()
            .insert(server_id.clone(), new_server.clone());

        Ok(new_server)
    }

    async fn connect(&self, server_node: &ServerNode) -> Result<ServerConnection, RpcError> {
        let url = server_node.url();
        let transport = Transport::connect(&url, self.timeout)
            .await
            .map_err(|error| ConnectionError(error.to_string()))?;

        let messenger = ServerConnectionInner::new(
            BufStream::new(transport),
            self.max_message_size,
            self.client_id.clone(),
        );
        Ok(ServerConnection::new(messenger))
    }
}

#[derive(Debug)]
struct Response {
    #[allow(dead_code)]
    header: ResponseHeader,
    data: Cursor<Vec<u8>>,
}

#[derive(Debug)]
struct ActiveRequest {
    channel: Sender<Result<Response, RpcError>>,
}

#[derive(Debug)]
enum ConnectionState {
    /// Currently active requests by request ID.
    ///
    /// An active request is one that got prepared or send but the response wasn't received yet.
    RequestMap(HashMap<i32, ActiveRequest>),

    /// One or our streams died and we are unable to process any more requests.
    Poison(Arc<RpcError>),
}

impl ConnectionState {
    fn poison(&mut self, err: RpcError) -> Arc<RpcError> {
        match self {
            Self::RequestMap(map) => {
                let err = Arc::new(err);

                // inform all active requests
                for (_request_id, active_request) in map.drain() {
                    // it's OK if the other side is gone
                    active_request
                        .channel
                        .send(Err(RpcError::Poisoned(Arc::clone(&err))))
                        .ok();
                }
                *self = Self::Poison(Arc::clone(&err));
                err
            }
            Self::Poison(e) => {
                // already poisoned, used existing error
                Arc::clone(e)
            }
        }
    }
}

#[derive(Debug)]
pub struct ServerConnectionInner<RW> {
    /// The half of the stream that we use to send data TO the broker.
    ///
    /// This will be used by [`request`](Self::request) to queue up messages.
    stream_write: Arc<AsyncMutex<WriteHalf<RW>>>,

    client_id: Arc<str>,

    request_id: AtomicI32,

    state: Arc<Mutex<ConnectionState>>,

    join_handle: JoinHandle<()>,
}

impl<RW> ServerConnectionInner<RW>
where
    RW: AsyncRead + AsyncWrite + Send + 'static,
{
    pub fn new(stream: RW, max_message_size: usize, client_id: Arc<str>) -> Self {
        let (stream_read, stream_write) = tokio::io::split(stream);
        let state = Arc::new(Mutex::new(ConnectionState::RequestMap(HashMap::default())));
        let state_captured = Arc::clone(&state);

        let join_handle = tokio::spawn(async move {
            let mut stream_read = stream_read;
            loop {
                match stream_read.read_message(max_message_size).await {
                    Ok(msg) => {
                        // message was read, so all subsequent errors should not poison the whole stream
                        let mut cursor = Cursor::new(msg);
                        let header =
                            match ResponseHeader::read_versioned(&mut cursor, ApiVersion(0)) {
                                Ok(header) => header,
                                Err(e) => {
                                    warn!(%e, "Cannot read message header, ignoring message");
                                    continue;
                                }
                            };

                        let active_request = match state_captured.lock().deref_mut() {
                            ConnectionState::RequestMap(map) => {
                                match map.remove(&header.request_id) {
                                    Some(active_request) => active_request,
                                    _ => {
                                        warn!(
                                            request_id = header.request_id,
                                            "Got response for unknown request",
                                        );
                                        continue;
                                    }
                                }
                            }
                            ConnectionState::Poison(_) => {
                                // stream is poisoned, no need to anything
                                return;
                            }
                        };

                        // we don't care if the other side is gone
                        active_request
                            .channel
                            .send(Ok(Response {
                                header,
                                data: cursor,
                            }))
                            .ok();
                    }
                    Err(e) => {
                        state_captured.lock().poison(RpcError::ReadMessageError(e));
                        return;
                    }
                }
            }
        });

        Self {
            stream_write: Arc::new(AsyncMutex::new(stream_write)),
            client_id,
            request_id: AtomicI32::new(0),
            state,
            join_handle,
        }
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, RpcError>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);
        let header = RequestHeader {
            request_api_key: R::API_KEY,
            request_api_version: ApiVersion(0),
            request_id,
            client_id: Some(String::from(self.client_id.as_ref())),
        };

        let header_version = ApiVersion(0);

        let body_api_version = ApiVersion(0);

        let mut buf = Vec::new();
        // write header
        header.write_versioned(&mut buf, header_version)?;
        // write message body
        msg.write_versioned(&mut buf, body_api_version)?;

        let (tx, rx) = channel();

        // to prevent stale data in inner state, ensure that we would remove the request again if we are cancelled while
        // sending the request
        let _cleanup_on_cancel =
            CleanupRequestStateOnCancel::new(Arc::clone(&self.state), request_id);

        match self.state.lock().deref_mut() {
            ConnectionState::RequestMap(map) => {
                map.insert(request_id, ActiveRequest { channel: tx });
            }
            ConnectionState::Poison(e) => return Err(RpcError::Poisoned(Arc::clone(e))),
        }

        self.send_message(buf).await?;
        _cleanup_on_cancel.message_sent();
        let mut response = rx.await.expect("Who closed this channel?!")?;

        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)?;

        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RpcError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version: body_api_version,
            });
        }
        Ok(body)
    }

    async fn send_message(&self, msg: Vec<u8>) -> Result<(), RpcError> {
        match self.send_message_inner(msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // need to poison the stream because message framing might be out-of-sync
                let mut state = self.state.lock();
                Err(RpcError::Poisoned(state.poison(e)))
            }
        }
    }

    async fn send_message_inner(&self, msg: Vec<u8>) -> Result<(), RpcError> {
        let mut stream_write = Arc::clone(&self.stream_write).lock_owned().await;

        // use a wrapper so that cancellation doesn't cancel the send operation and leaves half-send messages on the wire
        let fut = CancellationSafeFuture::new(async move {
            stream_write.write_message(&msg).await?;
            stream_write.flush().await?;
            Ok(())
        });

        fut.await
    }
}

impl<RW> Drop for ServerConnectionInner<RW> {
    fn drop(&mut self) {
        // todo: should remove from server_connections map?
        self.join_handle.abort();
    }
}

struct CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that option however we also need a pinned
    /// box because once this wrapper is polled, it will be pinned in memory -- even during drop. Now the inner
    /// future does not necessarily implement `Unpin`, so we need a heap allocation to pin it in memory even when we
    /// move it out of this option.
    inner: Option<BoxFuture<'static, F::Output>>,
}

impl<F> CancellationSafeFuture<F>
where
    F: Future + Send,
{
    fn new(fut: F) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future + Send,
{
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match self.inner.as_mut().expect("no dropped").as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Helper that ensures that a request is removed when a request is cancelled before it was actually sent out.
struct CleanupRequestStateOnCancel {
    state: Arc<Mutex<ConnectionState>>,
    request_id: i32,
    message_sent: bool,
}

impl CleanupRequestStateOnCancel {
    /// Create new helper.
    ///
    /// You must call [`message_sent`](Self::message_sent) when the request was sent.
    fn new(state: Arc<Mutex<ConnectionState>>, request_id: i32) -> Self {
        Self {
            state,
            request_id,
            message_sent: false,
        }
    }

    /// Request was sent. Do NOT clean the state any longer.
    fn message_sent(mut self) {
        self.message_sent = true;
    }
}

impl Drop for CleanupRequestStateOnCancel {
    fn drop(&mut self) {
        if !self.message_sent {
            if let ConnectionState::RequestMap(map) = self.state.lock().deref_mut() {
                map.remove(&self.request_id);
            }
        }
    }
}
