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
use crate::error::Error;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::error::RpcError;
use crate::rpc::error::RpcError::ConnectionError;
use crate::rpc::frame::{AsyncMessageRead, AsyncMessageWrite};
use crate::rpc::message::{
    ReadVersionedType, RequestBody, RequestHeader, ResponseHeader, WriteVersionedType,
};
use crate::rpc::transport::Transport;
use futures::future::BoxFuture;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fmt;
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

pub type MessengerTransport = ServerConnectionInner<BufStream<Transport>>;

pub type ServerConnection = Arc<MessengerTransport>;

// Matches Java's ExponentialBackoff(100ms initial, 2x multiplier, 5000ms max, 0.2 jitter).
const AUTH_INITIAL_BACKOFF_MS: f64 = 100.0;
const AUTH_MAX_BACKOFF_MS: f64 = 5000.0;
const AUTH_BACKOFF_MULTIPLIER: f64 = 2.0;
const AUTH_JITTER: f64 = 0.2;

#[derive(Clone)]
pub struct SaslConfig {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslConfig")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct RpcClient {
    connections: RwLock<HashMap<String, ServerConnection>>,
    client_id: Arc<str>,
    timeout: Option<Duration>,
    max_message_size: usize,
    sasl_config: Option<SaslConfig>,
}

impl RpcClient {
    pub fn new() -> Self {
        RpcClient {
            connections: Default::default(),
            client_id: Arc::from(""),
            timeout: None,
            max_message_size: usize::MAX,
            sasl_config: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_sasl(mut self, username: String, password: String) -> Self {
        self.sasl_config = Some(SaslConfig { username, password });
        self
    }

    pub async fn get_connection(
        &self,
        server_node: &ServerNode,
    ) -> Result<ServerConnection, Error> {
        let server_id = server_node.uid();
        {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(server_id).cloned() {
                if !conn.is_poisoned() {
                    return Ok(conn);
                }
            }
        }
        let new_server = self.connect(server_node).await?;
        {
            let mut connections = self.connections.write();
            if let Some(race_conn) = connections.get(server_id) {
                if !race_conn.is_poisoned() {
                    return Ok(race_conn.clone());
                }
            }

            connections.insert(server_id.to_owned(), new_server.clone());
        }
        Ok(new_server)
    }

    async fn connect(&self, server_node: &ServerNode) -> Result<ServerConnection, Error> {
        let url = server_node.url();
        let transport = Transport::connect(&url, self.timeout)
            .await
            .map_err(|error| ConnectionError(error.to_string()))?;

        let messenger = ServerConnectionInner::new(
            BufStream::new(transport),
            self.max_message_size,
            self.client_id.clone(),
        );
        let connection = ServerConnection::new(messenger);

        if let Some(ref sasl) = self.sasl_config {
            Self::authenticate(&connection, &sasl.username, &sasl.password).await?;
        }

        Ok(connection)
    }

    /// Perform SASL/PLAIN authentication handshake.
    ///
    /// Retries on `RetriableAuthenticateException` with exponential backoff
    /// (matching Java's unbounded retry behaviour). Non-retriable errors
    /// (wrong password, unknown user) propagate immediately as
    /// `Error::FlussAPIError` with the original error code.
    async fn authenticate(
        connection: &ServerConnection,
        username: &str,
        password: &str,
    ) -> Result<(), Error> {
        use crate::rpc::fluss_api_error::FlussError;
        use crate::rpc::message::AuthenticateRequest;
        use rand::Rng;

        let initial_request = AuthenticateRequest::new_plain(username, password);
        let mut retry_count: u32 = 0;

        loop {
            let request = initial_request.clone();
            let result = connection.request(request).await;

            match result {
                Ok(response) => {
                    // Check for server challenge (multi-round auth).
                    // PLAIN mechanism never sends a challenge, but we handle it
                    // for protocol correctness matching Java's handleAuthenticateResponse.
                    if let Some(challenge) = response.challenge {
                        let challenge_req = AuthenticateRequest::from_challenge("PLAIN", challenge);
                        connection.request(challenge_req).await?;
                    }
                    return Ok(());
                }
                Err(Error::FlussAPIError { ref api_error })
                    if FlussError::for_code(api_error.code)
                        == FlussError::RetriableAuthenticateException =>
                {
                    retry_count += 1;
                    // Cap the exponent like Java's ExponentialBackoff.expMax so that
                    // jitter still produces a range at steady state instead of being
                    // clamped to AUTH_MAX_BACKOFF_MS.
                    let exp_max = (AUTH_MAX_BACKOFF_MS / AUTH_INITIAL_BACKOFF_MS).log2();
                    let exp = ((retry_count as f64) - 1.0).min(exp_max);
                    let term = AUTH_INITIAL_BACKOFF_MS * AUTH_BACKOFF_MULTIPLIER.powf(exp);
                    let jitter_factor =
                        1.0 - AUTH_JITTER + rand::rng().random::<f64>() * (2.0 * AUTH_JITTER);
                    let backoff_ms = (term * jitter_factor) as u64;
                    log::warn!(
                        "SASL authentication retriable failure (attempt {retry_count}), \
                         retrying in {backoff_ms}ms: {}",
                        api_error.message
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                // Server-side auth errors (wrong password, unknown user, etc.)
                // propagate with their original error code preserved.
                Err(e) => return Err(e),
            }
        }
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
                                Err(err) => {
                                    log::warn!(
                                        "Cannot read message header, ignoring message: {err:?}"
                                    );
                                    continue;
                                }
                            };

                        let active_request = match state_captured.lock().deref_mut() {
                            ConnectionState::RequestMap(map) => {
                                match map.remove(&header.request_id) {
                                    Some(active_request) => active_request,
                                    _ => {
                                        log::warn!(
                                            request_id:% = header.request_id;
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

    fn is_poisoned(&self) -> bool {
        let guard = self.state.lock();
        matches!(*guard, ConnectionState::Poison(_))
    }

    pub async fn request<R>(&self, msg: R) -> Result<R::ResponseBody, Error>
    where
        R: RequestBody + Send + WriteVersionedType<Vec<u8>>,
        R::ResponseBody: ReadVersionedType<Cursor<Vec<u8>>>,
    {
        let api_label = crate::metrics::api_key_label(R::API_KEY);
        let start = std::time::Instant::now();
        let record_completion_metrics = |label: &'static str, response_bytes: u64| {
            metrics::counter!(
                crate::metrics::CLIENT_RESPONSES_TOTAL,
                crate::metrics::LABEL_API_KEY => label
            )
            .increment(1);
            metrics::counter!(
                crate::metrics::CLIENT_BYTES_RECEIVED_TOTAL,
                crate::metrics::LABEL_API_KEY => label
            )
            .increment(response_bytes);
            metrics::gauge!(
                crate::metrics::CLIENT_REQUESTS_IN_FLIGHT,
                crate::metrics::LABEL_API_KEY => label
            )
            .decrement(1.0);
            metrics::histogram!(
                crate::metrics::CLIENT_REQUEST_LATENCY_MS,
                crate::metrics::LABEL_API_KEY => label
            )
            .record(start.elapsed().as_secs_f64() * 1000.0);
        };

        let request_id = self.request_id.fetch_add(1, Ordering::SeqCst) & 0x7FFFFFFF;
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
        header
            .write_versioned(&mut buf, header_version)
            .map_err(RpcError::WriteMessageError)?;
        // write message body
        msg.write_versioned(&mut buf, body_api_version)
            .map_err(RpcError::WriteMessageError)?;

        let (tx, rx) = channel();

        // to prevent stale data in inner state, ensure that we would remove the request again if we are cancelled while
        // sending the request
        let _cleanup_on_cancel =
            CleanupRequestStateOnCancel::new(Arc::clone(&self.state), request_id);

        match self.state.lock().deref_mut() {
            ConnectionState::RequestMap(map) => {
                map.insert(request_id, ActiveRequest { channel: tx });
            }
            ConnectionState::Poison(e) => return Err(RpcError::Poisoned(Arc::clone(e)).into()),
        }

        // Guard to decrement in-flight on cancellation, panic, or any exit without
        // explicitly calling record_completion_metrics. Prevents gauge drift when
        // the request future is dropped (e.g. tokio::select! timeout).
        let mut in_flight_guard = if let Some(label) = api_label {
            metrics::counter!(
                crate::metrics::CLIENT_REQUESTS_TOTAL,
                crate::metrics::LABEL_API_KEY => label
            )
            .increment(1);
            metrics::counter!(
                crate::metrics::CLIENT_BYTES_SENT_TOTAL,
                crate::metrics::LABEL_API_KEY => label
            )
            .increment(buf.len() as u64);
            metrics::gauge!(
                crate::metrics::CLIENT_REQUESTS_IN_FLIGHT,
                crate::metrics::LABEL_API_KEY => label
            )
            .increment(1.0);
            Some(scopeguard::guard(label, |l| {
                metrics::gauge!(
                    crate::metrics::CLIENT_REQUESTS_IN_FLIGHT,
                    crate::metrics::LABEL_API_KEY => l
                )
                .decrement(1.0);
            }))
        } else {
            None
        };

        let mut disarm_in_flight_guard = || {
            if let Some(guard) = in_flight_guard.take() {
                let _ = scopeguard::ScopeGuard::into_inner(guard);
            }
        };

        if let Err(e) = self.send_message(buf).await {
            if let Some(label) = api_label {
                disarm_in_flight_guard();
                record_completion_metrics(label, 0);
            }
            return Err(e.into());
        }
        _cleanup_on_cancel.message_sent();
        let mut response = match rx.await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                if let Some(label) = api_label {
                    disarm_in_flight_guard();
                    record_completion_metrics(label, 0);
                }
                return Err(e.into());
            }
            Err(e) => {
                if let Some(label) = api_label {
                    disarm_in_flight_guard();
                    record_completion_metrics(label, 0);
                }
                return Err(Error::UnexpectedError {
                    message: "Got recvError, some one close the channel".to_string(),
                    source: Some(Box::new(e)),
                });
            }
        };

        let response_bytes = response.data.get_ref().len() as u64;
        if let Some(label) = api_label {
            disarm_in_flight_guard();
            record_completion_metrics(label, response_bytes);
        }

        if let Some(error_response) = response.header.error_response {
            return Err(Error::FlussAPIError {
                api_error: crate::rpc::ApiError::from(error_response),
            });
        }

        let body = R::ResponseBody::read_versioned(&mut response.data, body_api_version)
            .map_err(RpcError::ReadMessageError)?;

        let read_bytes = response.data.position();
        let message_bytes = response.data.into_inner().len() as u64;
        if read_bytes != message_bytes {
            return Err(RpcError::TooMuchData {
                message_size: message_bytes,
                read: read_bytes,
                api_key: R::API_KEY,
                api_version: body_api_version,
            }
            .into());
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
        let inner = self
            .inner
            .as_mut()
            .expect("CancellationSafeFuture polled after completion");

        match inner.as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                self.inner = None; // Prevent re-polling
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> Drop for CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
{
    fn drop(&mut self) {
        // If the future hasn't finished yet, we must ensure it completes in the background.
        // This prevents leaving half-sent messages on the wire if the caller cancels the request.
        if let Some(fut) = self.inner.take() {
            // Attempt to get a handle to the current Tokio runtime.
            // This avoids a panic if the runtime has already shut down.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = fut.await;
                });
            } else {
                // Fallback: If no runtime is active, we cannot spawn.
                // At this point, the future 'fut' will be dropped.
                // Since the runtime is likely shutting down anyway,
                // the underlying connection is probably being closed.
                warn!("Tokio runtime not found during drop; background task cancelled.");
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::rpc::ApiKey;
    use crate::rpc::api_version::ApiVersion;
    use crate::rpc::frame::{ReadError, WriteError};
    use crate::rpc::message::{ReadVersionedType, RequestBody, WriteVersionedType};
    use metrics_util::debugging::DebuggingRecorder;
    use std::sync::OnceLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
    use tokio::sync::Mutex as AsyncMutex;

    // -- Test-only request/response types --------------------------------

    struct TestProduceRequest;
    struct TestProduceResponse;

    impl RequestBody for TestProduceRequest {
        type ResponseBody = TestProduceResponse;
        const API_KEY: ApiKey = ApiKey::ProduceLog;
        const REQUEST_VERSION: ApiVersion = ApiVersion(0);
    }

    impl WriteVersionedType<Vec<u8>> for TestProduceRequest {
        fn write_versioned(&self, _w: &mut Vec<u8>, _v: ApiVersion) -> Result<(), WriteError> {
            Ok(())
        }
    }

    impl ReadVersionedType<Cursor<Vec<u8>>> for TestProduceResponse {
        fn read_versioned(_r: &mut Cursor<Vec<u8>>, _v: ApiVersion) -> Result<Self, ReadError> {
            Ok(TestProduceResponse)
        }
    }

    struct TestMetadataRequest;
    struct TestMetadataResponse;

    impl RequestBody for TestMetadataRequest {
        type ResponseBody = TestMetadataResponse;
        const API_KEY: ApiKey = ApiKey::MetaData;
        const REQUEST_VERSION: ApiVersion = ApiVersion(0);
    }

    impl WriteVersionedType<Vec<u8>> for TestMetadataRequest {
        fn write_versioned(&self, _w: &mut Vec<u8>, _v: ApiVersion) -> Result<(), WriteError> {
            Ok(())
        }
    }

    impl ReadVersionedType<Cursor<Vec<u8>>> for TestMetadataResponse {
        fn read_versioned(_r: &mut Cursor<Vec<u8>>, _v: ApiVersion) -> Result<Self, ReadError> {
            Ok(TestMetadataResponse)
        }
    }

    // -- Mock server -----------------------------------------------------

    /// Reads framed requests and echoes back minimal success responses.
    async fn mock_echo_server(mut stream: tokio::io::DuplexStream) {
        loop {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }

            // Header layout: api_key(2) + api_version(2) + request_id(4)
            let request_id = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);

            // Response: resp_type(1, 0=success) + request_id(4)
            let mut resp = Vec::with_capacity(5);
            resp.push(0u8);
            resp.extend_from_slice(&request_id.to_be_bytes());

            let resp_len = (resp.len() as i32).to_be_bytes();
            if stream.write_all(&resp_len).await.is_err()
                || stream.write_all(&resp).await.is_err()
                || stream.flush().await.is_err()
            {
                return;
            }
        }
    }

    /// Reads framed requests and echoes back error responses (resp_type=1).
    async fn mock_error_server(mut stream: tokio::io::DuplexStream) {
        use prost::Message;

        loop {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }

            let request_id = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);

            let err = crate::proto::ErrorResponse {
                error_code: 1,
                error_message: Some("test error".to_string()),
            };
            let mut err_buf = Vec::new();
            err.encode(&mut err_buf).expect("ErrorResponse encode");

            let mut resp = Vec::with_capacity(5 + err_buf.len());
            resp.push(1u8); // ERROR_RESPONSE
            resp.extend_from_slice(&request_id.to_be_bytes());
            resp.extend(err_buf);

            let resp_len = (resp.len() as i32).to_be_bytes();
            if stream.write_all(&resp_len).await.is_err()
                || stream.write_all(&resp).await.is_err()
                || stream.flush().await.is_err()
            {
                return;
            }
        }
    }

    // -- Recorder setup --------------------------------------------------

    /// Shared test recorder (installed once per test binary).
    static TEST_SNAPSHOTTER: OnceLock<metrics_util::debugging::Snapshotter> = OnceLock::new();
    static TEST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

    fn test_snapshotter() -> &'static metrics_util::debugging::Snapshotter {
        TEST_SNAPSHOTTER.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder
                .install()
                .expect("debugging recorder install should succeed in this test binary");
            snapshotter
        })
    }

    fn test_lock() -> &'static AsyncMutex<()> {
        TEST_LOCK.get_or_init(|| AsyncMutex::new(()))
    }

    // -- Tests -----------------------------------------------------------

    #[tokio::test]
    async fn request_records_metrics_for_reportable_api_key() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let response_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);

        conn.request(TestProduceRequest).await.unwrap();

        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let request_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let response_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        assert_eq!(
            request_after - request_before,
            1,
            "produce_log request counter should increment by 1"
        );
        assert_eq!(
            response_after - response_before,
            1,
            "produce_log completion counter should increment by 1"
        );

        let has_latency_sample = after.iter().any(|(key, _, _, value)| {
            key.key().name() == crate::metrics::CLIENT_REQUEST_LATENCY_MS
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log")
                && matches!(value, metrics_util::debugging::DebugValue::Histogram(_))
        });
        assert!(
            has_latency_sample,
            "request latency histogram should be recorded for produce_log"
        );
    }

    #[tokio::test]
    async fn request_skips_metrics_for_non_reportable_api_key() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_echo_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));
        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_sum_before: u64 = before
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .sum();
        let response_sum_before: u64 = before
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .sum();

        conn.request(TestMetadataRequest).await.unwrap();

        let snapshot: Vec<_> = snapshotter.snapshot().into_vec();
        let request_sum_after: u64 = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .sum();
        let response_sum_after: u64 = snapshot
            .iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .sum();
        assert_eq!(
            request_sum_after, request_sum_before,
            "non-reportable API keys must not change request counters"
        );
        assert_eq!(
            response_sum_after, response_sum_before,
            "non-reportable API keys must not change response counters"
        );

        // No metric entry should carry a non-reportable API key label.
        let non_reportable = snapshot.iter().any(|(key, _, _, _)| {
            key.key()
                .labels()
                .any(|l| l.key() == crate::metrics::LABEL_API_KEY && l.value() == "metadata")
        });
        assert!(
            !non_reportable,
            "non-reportable API keys must not appear in metrics"
        );
    }

    #[tokio::test]
    async fn request_records_completion_metrics_when_send_fails() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(64);
        drop(server); // force write failure on request path
        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let request_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let response_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let bytes_received_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_BYTES_RECEIVED_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let result = conn.request(TestProduceRequest).await;
        assert!(
            result.is_err(),
            "request should fail when transport is closed"
        );
        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let request_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let response_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let bytes_received_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_BYTES_RECEIVED_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let inflight_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_IN_FLIGHT || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Gauge(v) => Some(v.into_inner()),
                    _ => None,
                }
            })
            .unwrap_or(0.0);

        assert_eq!(
            request_after - request_before,
            1,
            "failed request should still count as request"
        );
        assert_eq!(
            response_after - response_before,
            1,
            "failed request should still count as a completion like Java ConnectionMetrics"
        );
        assert_eq!(
            bytes_received_after - bytes_received_before,
            0,
            "failed send should record zero received bytes"
        );
        assert_eq!(
            inflight_after, 0.0,
            "in-flight gauge must return to zero after failure"
        );
    }

    #[tokio::test]
    async fn request_records_completion_metrics_when_server_returns_api_error() {
        let _test_guard = test_lock().lock().await;
        let snapshotter = test_snapshotter();

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(mock_error_server(server));

        let conn = ServerConnectionInner::new(BufStream::new(client), usize::MAX, Arc::from("t"));

        let before: Vec<_> = snapshotter.snapshot().into_vec();
        let response_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let bytes_received_before = before
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_BYTES_RECEIVED_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);

        let result = conn.request(TestProduceRequest).await;
        assert!(
            matches!(result, Err(Error::FlussAPIError { .. })),
            "request should fail with FlussAPIError when server returns error_response"
        );

        let after: Vec<_> = snapshotter.snapshot().into_vec();
        let response_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_RESPONSES_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let bytes_received_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_BYTES_RECEIVED_TOTAL || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0);
        let inflight_after = after
            .iter()
            .find_map(|(key, _, _, value)| {
                let has_label = key.key().labels().any(|l| {
                    l.key() == crate::metrics::LABEL_API_KEY && l.value() == "produce_log"
                });
                if key.key().name() != crate::metrics::CLIENT_REQUESTS_IN_FLIGHT || !has_label {
                    return None;
                }
                match value {
                    metrics_util::debugging::DebugValue::Gauge(v) => Some(v.into_inner()),
                    _ => None,
                }
            })
            .unwrap_or(0.0);

        assert_eq!(
            response_after - response_before,
            1,
            "API error response should count as completion like Java"
        );
        assert!(
            bytes_received_after > bytes_received_before,
            "API error response should record received bytes (error body)"
        );
        assert_eq!(
            inflight_after, 0.0,
            "in-flight gauge must return to zero after API error"
        );
    }
}
