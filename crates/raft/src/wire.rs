//! Wire protocol of the cluster port (docs/DESIGN.md §8).
//!
//! A connection carries frames in both directions. A frame is a `u32`
//! big-endian payload length followed by that many bytes of a postcard
//! message. Frames longer than the configured maximum are rejected and the
//! connection is closed; nothing larger than the maximum is ever allocated.
//!
//! The dialer sends [`ClientMsg::Hello`] first; the listener answers with
//! [`ServerMsg::Hello`] (accepted or rejected) and then serves
//! [`ClientMsg::Request`]s, answering each with a [`ServerMsg::Response`]
//! carrying the same request id. Remote failures are values
//! ([`WireError`]), never a dropped connection.
//!
//! openraft's own error types are not sent as is: `StorageError` embeds a
//! recursive `AnyError` chain, and a deeply nested value could exhaust the
//! receiver's stack while decoding. [`WireError`] is flat.

use std::fmt;
use std::io;

use openraft::error::{Fatal, InstallSnapshotError, RaftError, SnapshotMismatch};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{ForwardRequest, ForwardResponse, NodeId, TypeConfig};

/// Version of this wire protocol, carried in the hellos.
pub const PROTOCOL_VERSION: u32 = 1;

/// Bytes of the length prefix.
pub const HEADER_LEN: usize = 4;

/// Default maximum payload size of one frame (32 MiB). It must exceed the
/// largest single log entry (a job body up to `-z` plus overhead) and
/// openraft's `snapshot_max_chunk_size`.
pub const DEFAULT_MAX_FRAME: usize = 32 << 20;

/// First frame on a connection, from the dialer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
    /// The dialer's node id. With TLS it must match the client certificate.
    pub from: NodeId,
    /// The node the dialer believes it is connected to.
    pub to: NodeId,
}

/// The listener's answer to [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerHello {
    Accepted {
        version: u32,
        node_id: NodeId,
    },
    /// The connection is closed right after this frame.
    Rejected {
        reason: String,
    },
}

/// Dialer → listener.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMsg {
    Hello(Hello),
    Request { id: u64, body: RpcRequest },
}

/// Listener → dialer.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMsg {
    Hello(ServerHello),
    Response { id: u64, body: RpcResponse },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    /// One chunk of openraft 0.9's chunked snapshot transfer.
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Forward(ForwardRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, WireError>),
    Vote(Result<VoteResponse<NodeId>, WireError>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, WireError>),
    Forward(Result<ForwardResponse, WireError>),
}

/// A remote failure, transported as a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireError {
    /// The remote Raft node failed (`RaftError::Fatal`).
    Fatal(WireFatal),
    /// `InstallSnapshotError::SnapshotMismatch`: the sender restarts the
    /// snapshot from offset 0.
    SnapshotMismatch(SnapshotMismatch),
    /// The listener refused the request (for example a forward whose
    /// `from` is not the authenticated peer).
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireFatal {
    Stopped,
    Panicked,
    /// A storage error, reduced to its message.
    Storage(String),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Fatal(WireFatal::Stopped) => f.write_str("remote raft stopped"),
            WireError::Fatal(WireFatal::Panicked) => f.write_str("remote raft panicked"),
            WireError::Fatal(WireFatal::Storage(m)) => write!(f, "remote storage error: {m}"),
            WireError::SnapshotMismatch(m) => write!(f, "snapshot mismatch: {m}"),
            WireError::Rejected(m) => write!(f, "rejected: {m}"),
        }
    }
}

impl std::error::Error for WireError {}

impl WireFatal {
    pub fn from_fatal(f: &Fatal<NodeId>) -> Self {
        match f {
            Fatal::Stopped => WireFatal::Stopped,
            Fatal::Panicked => WireFatal::Panicked,
            Fatal::StorageError(e) => WireFatal::Storage(e.to_string()),
        }
    }

    pub fn into_fatal(self) -> Fatal<NodeId> {
        match self {
            WireFatal::Stopped => Fatal::Stopped,
            WireFatal::Panicked => Fatal::Panicked,
            WireFatal::Storage(m) => Fatal::StorageError(StorageError::from(StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                &io::Error::other(m),
            ))),
        }
    }
}

impl WireError {
    /// From the error of `Raft::append_entries` / `Raft::vote`.
    pub fn from_raft(e: &RaftError<NodeId>) -> Self {
        match e {
            RaftError::APIError(never) => match *never {},
            RaftError::Fatal(f) => WireError::Fatal(WireFatal::from_fatal(f)),
        }
    }

    /// From the error of `Raft::install_snapshot`.
    pub fn from_snapshot(e: &RaftError<NodeId, InstallSnapshotError>) -> Self {
        match e {
            RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m)) => {
                WireError::SnapshotMismatch(m.clone())
            }
            RaftError::Fatal(f) => WireError::Fatal(WireFatal::from_fatal(f)),
        }
    }
}

/// Why a frame could not be read or written.
#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    /// The frame's payload exceeds the maximum.
    TooLarge {
        len: usize,
        max: usize,
    },
    /// The stream ended inside a frame.
    Truncated,
    /// The payload is not a valid message.
    Decode(postcard::Error),
    /// The message could not be serialized.
    Encode(postcard::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "i/o error: {e}"),
            FrameError::TooLarge { len, max } => {
                write!(f, "frame of {len} bytes exceeds the maximum of {max}")
            }
            FrameError::Truncated => f.write_str("stream ended inside a frame"),
            FrameError::Decode(e) => write!(f, "invalid frame: {e}"),
            FrameError::Encode(e) => write!(f, "cannot encode frame: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Serializes `msg` into a complete frame (header included).
pub fn encode<T: Serialize>(msg: &T, max_frame: usize) -> Result<Vec<u8>, FrameError> {
    let buf = postcard::to_extend(msg, vec![0u8; HEADER_LEN]).map_err(FrameError::Encode)?;
    let len = buf.len() - HEADER_LEN;
    if len > max_frame {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame,
        });
    }
    let mut buf = buf;
    let header = u32::try_from(len)
        .map_err(|_| FrameError::TooLarge {
            len,
            max: max_frame,
        })?
        .to_be_bytes();
    buf[..HEADER_LEN].copy_from_slice(&header);
    Ok(buf)
}

/// Decodes one frame from the front of `buf`: `Ok(None)` if `buf` does not
/// hold a complete frame yet, otherwise the message and the bytes consumed.
/// An oversized length is rejected before its payload arrives.
pub fn decode<T: DeserializeOwned>(
    buf: &[u8],
    max_frame: usize,
) -> Result<Option<(T, usize)>, FrameError> {
    let Some(header) = buf.get(..HEADER_LEN) else {
        return Ok(None);
    };
    let len = frame_len(header, max_frame)?;
    let Some(payload) = buf.get(HEADER_LEN..HEADER_LEN + len) else {
        return Ok(None);
    };
    let msg = postcard::from_bytes(payload).map_err(FrameError::Decode)?;
    Ok(Some((msg, HEADER_LEN + len)))
}

fn frame_len(header: &[u8], max_frame: usize) -> Result<usize, FrameError> {
    let mut h = [0u8; HEADER_LEN];
    h.copy_from_slice(header);
    let len = u32::from_be_bytes(h) as usize;
    if len > max_frame {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame,
        });
    }
    Ok(len)
}

/// Reads one frame. `Ok(None)` on a clean end of stream before the first
/// header byte. The payload buffer grows with the bytes actually received,
/// so a peer announcing a large frame cannot make us allocate it up front.
pub async fn read_frame<R, T>(r: &mut R, max_frame: usize) -> Result<Option<T>, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; HEADER_LEN];
    let mut got = 0;
    while got < HEADER_LEN {
        let n = r.read(&mut header[got..]).await?;
        if n == 0 {
            return if got == 0 {
                Ok(None)
            } else {
                Err(FrameError::Truncated)
            };
        }
        got += n;
    }
    let len = frame_len(&header, max_frame)?;
    let mut payload = Vec::with_capacity(len.min(64 * 1024));
    let n = (&mut *r).take(len as u64).read_to_end(&mut payload).await?;
    if n < len {
        return Err(FrameError::Truncated);
    }
    postcard::from_bytes(&payload)
        .map(Some)
        .map_err(FrameError::Decode)
}

/// Writes one already-encoded frame (see [`encode`]) and flushes.
pub async fn write_frame<W>(w: &mut W, frame: &[u8]) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    w.write_all(frame).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, Request};
    use bstk_engine::EngineInput;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, SnapshotMeta, Vote};

    fn sample_append() -> ClientMsg {
        let leader = CommittedLeaderId::new(3, 1);
        ClientMsg::Request {
            id: 7,
            body: RpcRequest::AppendEntries(AppendEntriesRequest {
                vote: Vote::new_committed(3, 1),
                prev_log_id: Some(LogId::new(leader, 10)),
                entries: vec![Entry {
                    log_id: LogId::new(leader, 11),
                    payload: EntryPayload::Normal(Request {
                        now: 42,
                        op: Op::Conn {
                            seq: 1,
                            input: EngineInput::Connect(1 << 48),
                        },
                    }),
                }],
                leader_commit: Some(LogId::new(leader, 9)),
            }),
        }
    }

    #[test]
    fn round_trip_every_message_kind() {
        let msgs = vec![
            ClientMsg::Hello(Hello {
                version: PROTOCOL_VERSION,
                from: 2,
                to: 1,
            }),
            sample_append(),
            ClientMsg::Request {
                id: 8,
                body: RpcRequest::Vote(VoteRequest::new(Vote::new(4, 2), None)),
            },
            ClientMsg::Request {
                id: 9,
                body: RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                    vote: Vote::new_committed(3, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: "s-1".to_string(),
                    },
                    offset: 5,
                    data: vec![1, 2, 3],
                    done: false,
                }),
            },
            ClientMsg::Request {
                id: 10,
                body: RpcRequest::Forward(ForwardRequest {
                    from: 2,
                    items: vec![(2 << 48 | 5, 3, EngineInput::HalfClose(2 << 48 | 5))],
                }),
            },
        ];
        let mut stream = Vec::new();
        for m in &msgs {
            stream.extend(encode(m, DEFAULT_MAX_FRAME).expect("encode"));
        }
        let mut at = 0;
        for m in &msgs {
            let (got, used): (ClientMsg, usize) = decode(&stream[at..], DEFAULT_MAX_FRAME)
                .expect("decode")
                .expect("complete");
            assert_eq!(format!("{got:?}"), format!("{m:?}"));
            assert_eq!(
                &stream[at..at + used],
                &encode(&got, DEFAULT_MAX_FRAME).expect("re")[..]
            );
            at += used;
        }
        assert_eq!(at, stream.len());

        let responses = vec![
            ServerMsg::Hello(ServerHello::Accepted {
                version: PROTOCOL_VERSION,
                node_id: 1,
            }),
            ServerMsg::Hello(ServerHello::Rejected {
                reason: "no".into(),
            }),
            ServerMsg::Response {
                id: 1,
                body: RpcResponse::AppendEntries(Ok(AppendEntriesResponse::Success)),
            },
            ServerMsg::Response {
                id: 2,
                body: RpcResponse::Vote(Err(WireError::Fatal(WireFatal::Storage("x".into())))),
            },
            ServerMsg::Response {
                id: 3,
                body: RpcResponse::InstallSnapshot(Err(WireError::SnapshotMismatch(
                    SnapshotMismatch {
                        expect: openraft::SnapshotSegmentId {
                            id: "a".into(),
                            offset: 0,
                        },
                        got: openraft::SnapshotSegmentId {
                            id: "a".into(),
                            offset: 9,
                        },
                    },
                ))),
            },
            ServerMsg::Response {
                id: 4,
                body: RpcResponse::Forward(Ok(ForwardResponse::NotLeader { leader: Some(3) })),
            },
        ];
        for m in &responses {
            let frame = encode(m, DEFAULT_MAX_FRAME).expect("encode");
            let (got, used): (ServerMsg, usize) = decode(&frame, DEFAULT_MAX_FRAME)
                .expect("decode")
                .expect("complete");
            assert_eq!(format!("{got:?}"), format!("{m:?}"));
            assert_eq!(used, frame.len());
        }
    }

    #[test]
    fn oversize_is_rejected_on_both_sides() {
        let msg = sample_append();
        let frame = encode(&msg, DEFAULT_MAX_FRAME).expect("encode");
        let len = frame.len() - HEADER_LEN;
        assert!(matches!(
            encode(&msg, len - 1),
            Err(FrameError::TooLarge { .. })
        ));
        // The receiver rejects on the header alone.
        assert!(matches!(
            decode::<ClientMsg>(&frame[..HEADER_LEN], len - 1),
            Err(FrameError::TooLarge { .. })
        ));
        let huge = u32::MAX.to_be_bytes();
        assert!(matches!(
            decode::<ClientMsg>(&huge, DEFAULT_MAX_FRAME),
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[test]
    fn truncated_frames_are_incomplete() {
        let frame = encode(&sample_append(), DEFAULT_MAX_FRAME).expect("encode");
        for cut in 0..frame.len() {
            assert!(
                decode::<ClientMsg>(&frame[..cut], DEFAULT_MAX_FRAME)
                    .expect("no error")
                    .is_none()
            );
        }
    }

    #[test]
    fn garbage_is_a_decode_error() {
        let mut frame = vec![0, 0, 0, 3];
        frame.extend([0xff, 0xff, 0xff]);
        assert!(matches!(
            decode::<ClientMsg>(&frame, DEFAULT_MAX_FRAME),
            Err(FrameError::Decode(_))
        ));
        let empty = [0u8, 0, 0, 0];
        assert!(matches!(
            decode::<ServerMsg>(&empty, DEFAULT_MAX_FRAME),
            Err(FrameError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn async_reader_handles_eof_truncation_and_oversize() {
        let frame = encode(&sample_append(), DEFAULT_MAX_FRAME).expect("encode");
        // Two frames then clean EOF.
        let mut two = frame.clone();
        two.extend(&frame);
        let mut r = two.as_slice();
        for _ in 0..2 {
            let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("read");
            assert_eq!(format!("{m:?}"), format!("{:?}", Some(sample_append())));
        }
        let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("eof");
        assert!(m.is_none());

        // EOF inside the header and inside the payload.
        for cut in [2, HEADER_LEN + 1, frame.len() - 1] {
            let mut r = &frame[..cut];
            let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
            assert!(matches!(e, Err(FrameError::Truncated)), "cut {cut}");
        }

        // Oversize announced length: rejected without reading the payload.
        let mut r = &u32::MAX.to_be_bytes()[..];
        let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
        assert!(matches!(e, Err(FrameError::TooLarge { .. })));

        // Garbage payload.
        let mut r = &[0u8, 0, 0, 2, 0xff, 0xff][..];
        let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
        assert!(matches!(e, Err(FrameError::Decode(_))));
    }

    #[test]
    fn wire_errors_convert_back() {
        let e = WireError::from_raft(&RaftError::Fatal(Fatal::Stopped));
        assert_eq!(e, WireError::Fatal(WireFatal::Stopped));
        let WireError::Fatal(f) = e else {
            panic!("fatal expected")
        };
        assert!(matches!(f.into_fatal(), Fatal::Stopped));
        let s = WireFatal::Storage("disk".into()).into_fatal();
        assert!(s.to_string().contains("disk"), "{s}");
    }
}
