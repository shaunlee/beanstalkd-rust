//! The opt-in HTTP listener (`[http]`; docs/PLAN.md §5.3 decision 5): a
//! small HTTP/1.1 server with four read-only endpoints.
//!
//! | request | response |
//! |---|---|
//! | `GET /healthz` | 200 `ok` while the process runs |
//! | `GET /readyz` | 503 until the engine is recovered (binlog replayed) and the listeners accept connections, then 200 `ready`; in cluster mode also 503 whenever no leader is known or this node lags behind the commit index it learned |
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
//! Engine work is bounded (P2 security review, finding F3): a snapshot
//! holds at most `http.max_tube_series + 1` tubes (one more than is shown,
//! to tell whether some were left out), and one snapshot is reused by
//! `/metrics` and `/admin` for up to `http.snapshot_min_interval`, so the
//! engine takes at most one snapshot per interval however often they are
//! requested (concurrent requests that miss the cache share one snapshot).
//!
//! Resources are bounded (finding F4): at most [`MAX_CONNECTIONS`]
//! connections are served at once (more wait in the listen backlog),
//! request headers must arrive within [`HEADER_READ_TIMEOUT`], every
//! connection is closed after one request (no keep-alive) and
//! [`CONNECTION_TIMEOUT`] after it was accepted at the latest, and request
//! bodies are never read. Requests are routed once their headers are
//! parsed: `/healthz` and `/readyz` are answered at once, without waiting
//! for anything else, while `/metrics` and `/admin` first take one of
//! [`MAX_SNAPSHOT_REQUESTS`] permits (held only while the response is
//! built, not while it is written), so monitoring clients can never hold
//! up health checks.
//!
//! Residual risk: a client that keeps [`MAX_CONNECTIONS`] connections open
//! without finishing their headers delays new connections, health checks
//! included, by up to [`HEADER_READ_TIMEOUT`] per round (the backlog is
//! served as those connections time out). The main mitigation is that the
//! HTTP listener is off by default and its documented address is
//! 127.0.0.1; expose it only to trusted networks.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::{Body, Incoming};
use hyper::header::{ALLOW, CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, oneshot, watch};

use bstk_engine::Snapshot;

use crate::config::HttpSettings;
use crate::engine_actor::{EngineHandle, EngineMsg};
use crate::metrics::{self, ClusterInfo};
use crate::pending::ServerCounters;

/// Concurrent HTTP connections served; further ones wait to be accepted.
pub const MAX_CONNECTIONS: usize = 256;
/// Time allowed for a request's headers to arrive.
pub const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);
/// Concurrent `/metrics` and `/admin` requests being answered.
pub const MAX_SNAPSHOT_REQUESTS: usize = 4;
/// Upper bound on waiting for one of the [`MAX_SNAPSHOT_REQUESTS`] permits.
const SNAPSHOT_PERMIT_TIMEOUT: Duration = Duration::from_secs(5);
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
    snapshot_min_interval: Duration,
    /// Limits concurrent `/metrics` and `/admin` requests.
    snapshot_permits: Semaphore,
    /// The last snapshot and when it arrived. Held across the engine round
    /// trip, so concurrent cache misses wait for (and share) one snapshot.
    cache: Mutex<Option<(Instant, Arc<Snapshot>)>>,
    /// Server-side counters (`/metrics`, `/admin` `"server_rs"`).
    counters: Arc<ServerCounters>,
    /// Cluster mode: readiness and the cluster figures. Set before
    /// `engine`.
    cluster: OnceLock<Arc<dyn ClusterInfo>>,
}

impl HttpState {
    pub fn new(settings: &HttpSettings, counters: Arc<ServerCounters>) -> Arc<HttpState> {
        Arc::new(HttpState {
            engine: OnceLock::new(),
            max_tube_series: settings.max_tube_series,
            snapshot_min_interval: settings.snapshot_min_interval,
            snapshot_permits: Semaphore::new(MAX_SNAPSHOT_REQUESTS),
            cache: Mutex::new(None),
            counters,
            cluster: OnceLock::new(),
        })
    }

    /// Cluster mode: `/readyz` also requires `info.ready()`, and
    /// `/metrics` and `/admin` add the cluster figures. Call before
    /// `set_ready`.
    pub fn set_cluster(&self, info: Arc<dyn ClusterInfo>) {
        let _ = self.cluster.set(info);
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
        // Health checks never wait for anything.
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => match state.engine.get() {
            Some(_) if state.cluster.get().is_none_or(|c| c.ready()) => {
                text(StatusCode::OK, "ready")
            }
            _ => not_ready(),
        },
        _ => monitoring(state, path == "/metrics").await,
    }
}

/// `/metrics` (`prometheus`) or `/admin`, under one of the
/// [`MAX_SNAPSHOT_REQUESTS`] permits. The permit is released when the
/// response is returned, before hyper writes it, so a client that reads
/// slowly does not hold it.
async fn monitoring(state: &HttpState, prometheus: bool) -> Response<Full<Bytes>> {
    if state.engine.get().is_none() {
        return not_ready();
    }
    let _permit =
        match tokio::time::timeout(SNAPSHOT_PERMIT_TIMEOUT, state.snapshot_permits.acquire()).await
        {
            Ok(Ok(permit)) => permit,
            // Timed out (or, never, the semaphore was closed).
            _ => return text(StatusCode::SERVICE_UNAVAILABLE, BUSY),
        };
    let snap = match snapshot(state).await {
        Ok(s) => s,
        Err(why) => return text(StatusCode::SERVICE_UNAVAILABLE, why),
    };
    let rs = state.counters.sample();
    let cluster = state.cluster.get().map(|c| c.stats());
    if prometheus {
        let mut text = metrics::render_prometheus(&snap, state.max_tube_series, &rs);
        if let Some(c) = &cluster {
            metrics::render_cluster_prometheus(&mut text, c);
        }
        body(StatusCode::OK, PROMETHEUS_CONTENT_TYPE, text)
    } else {
        let mut text = metrics::render_admin_json(&snap, state.max_tube_series, &rs);
        if let Some(c) = &cluster {
            metrics::append_cluster_json(&mut text, c);
        }
        body(StatusCode::OK, JSON_CONTENT_TYPE, text)
    }
}

/// A snapshot at most `snapshot_min_interval` old, from the cache or else
/// from the engine actor, or why there is none (the body of a 503
/// response). It holds at most `max_tube_series + 1` tubes.
async fn snapshot(state: &HttpState) -> Result<Arc<Snapshot>, &'static str> {
    let Some(engine) = state.engine.get() else {
        return Err(NOT_READY);
    };
    let mut cache = state.cache.lock().await;
    if let Some((at, snap)) = cache.as_ref()
        && at.elapsed() < state.snapshot_min_interval
    {
        return Ok(Arc::clone(snap));
    }
    let (reply, rx) = oneshot::channel();
    let msg = EngineMsg::Snapshot {
        max_tubes: state.max_tube_series.saturating_add(1),
        reply,
    };
    if engine.send(msg).is_err() {
        return Err(ENGINE_UNAVAILABLE);
    }
    match tokio::time::timeout(SNAPSHOT_TIMEOUT, rx).await {
        Ok(Ok(s)) => {
            let snap = Arc::new(s);
            *cache = Some((Instant::now(), Arc::clone(&snap)));
            Ok(snap)
        }
        _ => Err(ENGINE_UNAVAILABLE),
    }
}

const NOT_READY: &str = "not ready";
const ENGINE_UNAVAILABLE: &str = "engine unavailable";
const BUSY: &str = "too many monitoring requests";

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bstk_proto::{StatsServer, StatsTube, TubeName};
    use tokio::sync::mpsc;

    use super::*;

    fn tube(name: &str) -> StatsTube {
        StatsTube {
            name: TubeName::new(name).unwrap(),
            current_jobs_urgent: 0,
            current_jobs_ready: 0,
            current_jobs_reserved: 0,
            current_jobs_delayed: 0,
            current_jobs_buried: 0,
            total_jobs: 0,
            current_using: 0,
            current_watching: 0,
            current_waiting: 0,
            cmd_delete: 0,
            cmd_pause_tube: 0,
            pause: 0,
            pause_time_left: 0,
        }
    }

    /// An `HttpState` whose engine is a stub task that answers every
    /// snapshot request (with `max_tubes` tubes) and counts them.
    fn state_with_stub(
        interval: Duration,
        max_tube_series: usize,
    ) -> (Arc<HttpState>, Arc<AtomicUsize>) {
        let settings = HttpSettings {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_tube_series,
            snapshot_min_interval: interval,
        };
        let state = HttpState::new(&settings, ServerCounters::new(1));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let EngineMsg::Snapshot { max_tubes, reply } = msg {
                    let n = counted.fetch_add(1, Ordering::SeqCst) as u64 + 1;
                    let tubes = (0..max_tubes.min(100))
                        .map(|i| tube(&format!("t{i}")))
                        .collect();
                    let server = StatsServer {
                        // Tells snapshots apart.
                        total_jobs: n,
                        ..StatsServer::default()
                    };
                    let _ = reply.send(Snapshot { server, tubes });
                }
            }
        });
        state.set_ready(EngineHandle::Task(tx));
        (state, requests)
    }

    #[tokio::test]
    async fn snapshots_are_cached_for_the_interval() {
        let (state, requests) = state_with_stub(Duration::from_secs(3600), 5);
        let a = snapshot(&state).await.unwrap();
        let b = snapshot(&state).await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 1, "one engine snapshot");
        assert!(Arc::ptr_eq(&a, &b));
        // Rendering both endpoints reuses it too.
        let _ = monitoring(&state, true).await;
        let _ = monitoring(&state, false).await;
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        // One more tube than the limit is asked for.
        assert_eq!(a.tubes.len(), 6);
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_snapshot() {
        let (state, requests) = state_with_stub(Duration::from_secs(3600), 5);
        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let state = Arc::clone(&state);
                tokio::spawn(async move { snapshot(&state).await.unwrap().server.total_jobs })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.unwrap(), 1);
        }
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_zero_interval_disables_the_cache() {
        let (state, requests) = state_with_stub(Duration::ZERO, 5);
        let a = snapshot(&state).await.unwrap();
        let b = snapshot(&state).await.unwrap();
        assert_eq!((a.server.total_jobs, b.server.total_jobs), (1, 2));
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_expired_snapshot_is_replaced() {
        let (state, requests) = state_with_stub(Duration::from_millis(100), 5);
        let _ = snapshot(&state).await.unwrap();
        let _ = snapshot(&state).await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let s = snapshot(&state).await.unwrap();
        assert_eq!(s.server.total_jobs, 2);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn admin_is_capped_at_max_tube_series() {
        let (state, _) = state_with_stub(Duration::ZERO, 5);
        let resp = monitoring(&state, false).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            text.contains("\"tube_limit\":5,\"tubes_truncated\":true"),
            "{text}"
        );
        assert_eq!(text.matches("\"name\":").count(), 5, "{text}");
    }

    #[tokio::test]
    async fn not_ready_before_the_engine() {
        let settings = HttpSettings {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_tube_series: 5,
            snapshot_min_interval: Duration::from_secs(1),
        };
        let state = HttpState::new(&settings, ServerCounters::new(1));
        assert_eq!(snapshot(&state).await.err(), Some(NOT_READY));
        assert_eq!(
            monitoring(&state, true).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
