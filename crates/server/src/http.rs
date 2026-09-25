//! The opt-in HTTP listener (`[http]`; docs/PLAN.md §5.3 decision 5): a
//! small HTTP/1.1 server with four read-only endpoints.
//!
//! | request | response |
//! |---|---|
//! | `GET /healthz` | 200 `ok` while the process runs |
//! | `GET /readyz` | 503 until the engine is recovered (binlog replayed) and the listeners accept connections, then 200 `ready` |
//! | `GET /metrics` | Prometheus text format (`metrics::render_prometheus`), 503 before ready |
//! | `GET /admin` | JSON (`metrics::render_admin_json`), 503 before ready |
//! | another path | 404 |
//! | another method | 405 (with `Allow: GET`) |
//! | a request with a body | 400 |
//!
//! It is started before the binlog replay, so `/healthz` and `/readyz`
//! answer during recovery. Snapshots come from the engine actor
//! (`EngineMsg::Snapshot`), so they are consistent with what `stats` and
//! `stats-tube` would report at that moment, without changing any counter.
//!
//! Resources are bounded: at most [`MAX_CONNECTIONS`] connections are
//! served at once (more wait in the listen backlog), request headers must
//! arrive within [`HEADER_READ_TIMEOUT`], every connection is closed after
//! one request (no keep-alive) and [`CONNECTION_TIMEOUT`] after it was
//! accepted at the latest, and request bodies are never read.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::{Body, Incoming};
use hyper::header::{ALLOW, CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot, watch};

use crate::engine_actor::{EngineHandle, EngineMsg};
use crate::metrics;

/// Concurrent HTTP connections served; further ones wait to be accepted.
pub const MAX_CONNECTIONS: usize = 64;
/// Time allowed for a request's headers to arrive.
pub const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on a connection's lifetime.
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on waiting for a snapshot from the engine actor.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const JSON_CONTENT_TYPE: &str = "application/json";
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// What the endpoints need; shared by every HTTP connection.
pub struct HttpState {
    /// Set once the engine is recovered and the listeners are bound: from
    /// then on the server is ready.
    engine: OnceLock<EngineHandle>,
    max_tube_series: usize,
}

impl HttpState {
    pub fn new(max_tube_series: usize) -> Arc<HttpState> {
        Arc::new(HttpState {
            engine: OnceLock::new(),
            max_tube_series,
        })
    }

    /// Marks the server ready (`/readyz` 200, `/metrics` and `/admin`
    /// served).
    pub fn set_ready(&self, engine: EngineHandle) {
        let _ = self.engine.set(engine);
    }
}

/// Accepts and serves HTTP connections until `stop` turns true (or its
/// sender is gone).
pub async fn serve(listener: TcpListener, state: Arc<HttpState>, mut stop: watch::Receiver<bool>) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let permit = tokio::select! {
            _ = stop.wait_for(|&s| s) => return,
            permit = Arc::clone(&permits).acquire_owned() => match permit {
                Ok(p) => p,
                // The semaphore is never closed.
                Err(_) => return,
            },
        };
        let (tcp, peer) = tokio::select! {
            _ = stop.wait_for(|&s| s) => return,
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!("HTTP accept() failed: {e}");
                    continue;
                }
            },
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _permit = permit;
            let service = service_fn(move |req| {
                let state = Arc::clone(&state);
                async move { Ok::<_, Infallible>(route(&state, &req).await) }
            });
            let conn = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .keep_alive(false)
                .serve_connection(TokioIo::new(tcp), service);
            match tokio::time::timeout(CONNECTION_TIMEOUT, conn).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::debug!(%peer, "HTTP connection error: {e}"),
                Err(_) => tracing::debug!(%peer, "HTTP connection timed out"),
            }
        });
    }
}

async fn route(state: &HttpState, req: &Request<Incoming>) -> Response<Full<Bytes>> {
    let path = req.uri().path();
    let known = matches!(path, "/healthz" | "/readyz" | "/metrics" | "/admin");
    if !known {
        return text(StatusCode::NOT_FOUND, "not found");
    }
    if req.method() != Method::GET {
        let mut resp = text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
        resp.headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET"));
        return resp;
    }
    // A GET has no body; refuse one rather than read it.
    if req.body().size_hint().upper() != Some(0) {
        return text(StatusCode::BAD_REQUEST, "request bodies are not accepted");
    }
    match path {
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => match state.engine.get() {
            Some(_) => text(StatusCode::OK, "ready"),
            None => not_ready(),
        },
        "/metrics" => match snapshot(state).await {
            Ok(s) => body(
                StatusCode::OK,
                PROMETHEUS_CONTENT_TYPE,
                metrics::render_prometheus(&s, state.max_tube_series),
            ),
            Err(why) => text(StatusCode::SERVICE_UNAVAILABLE, why),
        },
        _ => match snapshot(state).await {
            Ok(s) => body(
                StatusCode::OK,
                JSON_CONTENT_TYPE,
                metrics::render_admin_json(&s),
            ),
            Err(why) => text(StatusCode::SERVICE_UNAVAILABLE, why),
        },
    }
}

/// A snapshot from the engine actor, or why there is none (the body of a
/// 503 response).
async fn snapshot(state: &HttpState) -> Result<bstk_engine::Snapshot, &'static str> {
    let Some(engine) = state.engine.get() else {
        return Err(NOT_READY);
    };
    let (reply, rx) = oneshot::channel();
    if engine.send(EngineMsg::Snapshot { reply }).is_err() {
        return Err(ENGINE_UNAVAILABLE);
    }
    match tokio::time::timeout(SNAPSHOT_TIMEOUT, rx).await {
        Ok(Ok(s)) => Ok(s),
        _ => Err(ENGINE_UNAVAILABLE),
    }
}

const NOT_READY: &str = "not ready";
const ENGINE_UNAVAILABLE: &str = "engine unavailable";

fn not_ready() -> Response<Full<Bytes>> {
    text(StatusCode::SERVICE_UNAVAILABLE, NOT_READY)
}

fn text(status: StatusCode, msg: &'static str) -> Response<Full<Bytes>> {
    body(status, TEXT_CONTENT_TYPE, msg.to_owned())
}

fn body(status: StatusCode, content_type: &'static str, text: String) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from(text)));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}

/// Binds the HTTP listener (same socket options as the protocol
/// listeners).
pub fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    crate::listen(addr)
}
