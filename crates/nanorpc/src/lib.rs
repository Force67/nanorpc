//! RPC over nanobuf messages.
//!
//! The protocol is specified in `docs/protocol.md`: 16-byte frames over
//! TCP, calls multiplexed by id, every call ended by exactly one CLOSE
//! frame carrying a status. This crate is the Rust runtime: a blocking
//! [`Client`] and [`Server`] built only on `std`, multiplexing concurrent
//! calls over one connection with threads.
//!
//! Services are plain values, not codegen output:
//!
//! ```ignore
//! const SAY: Method<EchoRequest, EchoResponse> = Method::new("interop.Echo/say");
//!
//! let router = Router::new()
//!     .unary(SAY, |_ctx, req| Ok(EchoResponse { text: req.text }));
//! ```

mod client;
mod limit;
mod server;

pub use client::{Client, Streaming};
pub use nanorpc_wire::{Code, Status};
pub use server::{Context, Router, Server, Sink};

use std::marker::PhantomData;

/// A nanobuf message that can travel as an RPC payload. Implement it with
/// [`messages!`], which delegates to the generated `encode`/`decode`.
pub trait Message: Sized + Send + 'static {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(data: &[u8]) -> Result<Self, nanobuf::DecodeError>;
}

/// Implements [`Message`] for nanobuf-generated owned types.
#[macro_export]
macro_rules! messages {
    ($($ty:ty),* $(,)?) => {$(
        impl $crate::Message for $ty {
            fn to_bytes(&self) -> Vec<u8> {
                self.encode()
            }
            fn from_bytes(data: &[u8]) -> Result<Self, ::nanobuf::DecodeError> {
                Self::decode(data)
            }
        }
    )*};
}

/// A unary method: one request, one response. The id is the FNV-1a of the
/// path, computed at compile time; the path itself never crosses the wire.
pub struct Method<Req, Res> {
    pub path: &'static str,
    pub id: u64,
    types: PhantomData<fn(Req) -> Res>,
}

impl<Req: Message, Res: Message> Method<Req, Res> {
    pub const fn new(path: &'static str) -> Self {
        Method {
            path,
            id: nanorpc_wire::method_id(path),
            types: PhantomData,
        }
    }
}

// Manual impls: derive would bound Req/Res, which PhantomData does not need.
impl<Req, Res> Clone for Method<Req, Res> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<Req, Res> Copy for Method<Req, Res> {}

/// A server-streaming method: one request, any number of response
/// messages, then a status.
pub struct StreamMethod<Req, Res> {
    pub path: &'static str,
    pub id: u64,
    types: PhantomData<fn(Req) -> Res>,
}

impl<Req: Message, Res: Message> StreamMethod<Req, Res> {
    pub const fn new(path: &'static str) -> Self {
        StreamMethod {
            path,
            id: nanorpc_wire::method_id(path),
            types: PhantomData,
        }
    }
}

impl<Req, Res> Clone for StreamMethod<Req, Res> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<Req, Res> Copy for StreamMethod<Req, Res> {}

/// Client-side failure of one call.
#[derive(Debug)]
pub enum RpcError {
    /// The server closed the call with a non-OK status.
    Status(Status),
    /// The connection failed; the call's fate is unknown.
    Transport(std::io::Error),
    /// A payload did not decode as the expected message type.
    Decode(nanobuf::DecodeError),
    /// The peer violated the protocol (e.g. CLOSE(OK) without a response).
    Protocol(String),
}

impl RpcError {
    /// The status code, when the failure has one. Local timeouts map to
    /// `DEADLINE_EXCEEDED` so callers match on one code either way.
    pub fn code(&self) -> Option<Code> {
        match self {
            RpcError::Status(status) => Some(status.code),
            _ => None,
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Status(status) => write!(f, "call failed: {status}"),
            RpcError::Transport(err) => write!(f, "transport: {err}"),
            RpcError::Decode(err) => write!(f, "response did not decode: {err:?}"),
            RpcError::Protocol(what) => write!(f, "protocol violation: {what}"),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<Status> for RpcError {
    fn from(status: Status) -> RpcError {
        RpcError::Status(status)
    }
}
