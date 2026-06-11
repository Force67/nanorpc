//! Frame layer of the nanorpc protocol.
//!
//! Everything on a nanorpc connection is a 16-byte little-endian frame
//! header followed by `length` payload bytes; the full layout lives in
//! `docs/protocol.md`. This crate owns the byte-level pieces: the
//! connection preface, frame headers, the call prologue, status codes,
//! and method ids. It does no I/O beyond `std::io` traits, so the client,
//! the server, and any proxy share one definition of the bytes.

use std::io::{self, Read, Write};

/// Sent by both peers immediately after connecting: the protocol name and
/// a version byte. A peer that reads anything else hangs up.
pub const PREFACE: [u8; 8] = *b"nanorpc\x01";

/// Frames larger than this are a protocol violation. Large payloads are an
/// application decision; runtimes may lower the limit, never raise it.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

pub const FRAME_HEADER_LEN: usize = 16;
pub const CALL_PROLOGUE_LEN: usize = 12;

/// Frame type. Open: unknown kinds are a connection error, which is how
/// the protocol stays extensible without version sniffing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameKind(pub u8);

impl FrameKind {
    /// Opens a call. Payload: 12-byte prologue, then the request message.
    pub const CALL: FrameKind = FrameKind(0);
    /// One message on an open call, in either direction.
    pub const MESSAGE: FrameKind = FrameKind(1);
    /// Ends a call. `status` is the outcome; payload is a UTF-8 detail
    /// string. Every call ends with exactly one CLOSE from the server.
    pub const CLOSE: FrameKind = FrameKind(2);
    /// Client no longer wants the call. Best-effort; the server still
    /// sends CLOSE.
    pub const CANCEL: FrameKind = FrameKind(3);
    /// Keepalive probe carrying 8 opaque bytes, answered with PONG.
    pub const PING: FrameKind = FrameKind(4);
    pub const PONG: FrameKind = FrameKind(5);

    pub fn name(self) -> Option<&'static str> {
        Some(match self {
            FrameKind::CALL => "CALL",
            FrameKind::MESSAGE => "MESSAGE",
            FrameKind::CLOSE => "CLOSE",
            FrameKind::CANCEL => "CANCEL",
            FrameKind::PING => "PING",
            FrameKind::PONG => "PONG",
            _ => return None,
        })
    }
}

/// The 16-byte header preceding every payload.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FrameHeader {
    pub kind: FrameKind,
    /// Reserved, must be zero in version 1.
    pub flags: u8,
    /// Status code; meaningful only on CLOSE, zero elsewhere.
    pub status: u16,
    /// Client-assigned call id; zero on connection-level frames.
    pub call_id: u32,
    /// Payload length in bytes.
    pub length: u32,
}

impl FrameHeader {
    pub fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut bytes = [0u8; FRAME_HEADER_LEN];
        bytes[0] = self.kind.0;
        bytes[1] = self.flags;
        bytes[2..4].copy_from_slice(&self.status.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.call_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.length.to_le_bytes());
        // Bytes 12..16 are reserved and stay zero.
        bytes
    }

    pub fn decode(bytes: &[u8; FRAME_HEADER_LEN]) -> FrameHeader {
        FrameHeader {
            kind: FrameKind(bytes[0]),
            flags: bytes[1],
            status: u16::from_le_bytes([bytes[2], bytes[3]]),
            call_id: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            length: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        }
    }
}

/// First 12 payload bytes of every CALL frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CallPrologue {
    /// FNV-1a of the method path; see [`method_id`].
    pub method: u64,
    /// Milliseconds the caller is still willing to wait; zero means
    /// unbounded. Relative, so peers never need synchronized clocks.
    pub deadline_ms: u32,
}

impl CallPrologue {
    pub fn encode(&self) -> [u8; CALL_PROLOGUE_LEN] {
        let mut bytes = [0u8; CALL_PROLOGUE_LEN];
        bytes[..8].copy_from_slice(&self.method.to_le_bytes());
        bytes[8..].copy_from_slice(&self.deadline_ms.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8; CALL_PROLOGUE_LEN]) -> CallPrologue {
        CallPrologue {
            method: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            deadline_ms: u32::from_le_bytes(bytes[8..].try_into().unwrap()),
        }
    }
}

/// Routing id for a method path like `interop.Echo/say`: 64-bit FNV-1a.
/// Dispatch is one integer lookup; servers reject colliding registrations,
/// so a collision is a build-time event, never a wire ambiguity.
pub const fn method_id(path: &str) -> u64 {
    let bytes = path.as_bytes();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    hash
}

/// Call outcome code. Numerically identical to gRPC status codes, so
/// migrating services keep their dashboards and retry policies.
/// Open: peers preserve codes they do not know.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Code(pub u16);

impl Code {
    pub const OK: Code = Code(0);
    pub const CANCELLED: Code = Code(1);
    pub const UNKNOWN: Code = Code(2);
    pub const INVALID_ARGUMENT: Code = Code(3);
    pub const DEADLINE_EXCEEDED: Code = Code(4);
    pub const NOT_FOUND: Code = Code(5);
    pub const ALREADY_EXISTS: Code = Code(6);
    pub const PERMISSION_DENIED: Code = Code(7);
    pub const RESOURCE_EXHAUSTED: Code = Code(8);
    pub const FAILED_PRECONDITION: Code = Code(9);
    pub const ABORTED: Code = Code(10);
    pub const OUT_OF_RANGE: Code = Code(11);
    pub const UNIMPLEMENTED: Code = Code(12);
    pub const INTERNAL: Code = Code(13);
    pub const UNAVAILABLE: Code = Code(14);
    pub const DATA_LOSS: Code = Code(15);
    pub const UNAUTHENTICATED: Code = Code(16);

    pub fn name(self) -> Option<&'static str> {
        Some(match self.0 {
            0 => "ok",
            1 => "cancelled",
            2 => "unknown",
            3 => "invalid_argument",
            4 => "deadline_exceeded",
            5 => "not_found",
            6 => "already_exists",
            7 => "permission_denied",
            8 => "resource_exhausted",
            9 => "failed_precondition",
            10 => "aborted",
            11 => "out_of_range",
            12 => "unimplemented",
            13 => "internal",
            14 => "unavailable",
            15 => "data_loss",
            16 => "unauthenticated",
            _ => return None,
        })
    }
}

impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => f.write_str(name),
            None => write!(f, "code({})", self.0),
        }
    }
}

/// A call outcome: a [`Code`] plus a human-readable detail string. This is
/// what handlers return for failures and what CLOSE frames carry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Status {
    pub code: Code,
    pub message: String,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Status {
        Status {
            code,
            message: message.into(),
        }
    }

    pub fn ok() -> Status {
        Status::new(Code::OK, "")
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::OK
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Status {}

/// Reads one frame, enforcing `max_len` on the payload.
pub fn read_frame(reader: &mut impl Read, max_len: u32) -> io::Result<(FrameHeader, Vec<u8>)> {
    let mut header_bytes = [0u8; FRAME_HEADER_LEN];
    reader.read_exact(&mut header_bytes)?;
    let header = FrameHeader::decode(&header_bytes);
    if header.length > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {} bytes exceeds limit {max_len}", header.length),
        ));
    }
    let mut payload = vec![0u8; header.length as usize];
    reader.read_exact(&mut payload)?;
    Ok((header, payload))
}

/// Writes one frame. The single write call per frame is what makes frames
/// atomic units under a shared, mutex-guarded writer.
pub fn write_frame(
    writer: &mut impl Write,
    mut header: FrameHeader,
    payload: &[u8],
) -> io::Result<()> {
    header.length = payload.len() as u32;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&header.encode());
    frame.extend_from_slice(payload);
    writer.write_all(&frame)?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips() {
        let header = FrameHeader {
            kind: FrameKind::CLOSE,
            flags: 0,
            status: Code::NOT_FOUND.0,
            call_id: 7,
            length: 1234,
        };
        assert_eq!(FrameHeader::decode(&header.encode()), header);
    }

    #[test]
    fn prologue_round_trips() {
        let prologue = CallPrologue {
            method: method_id("interop.Echo/say"),
            deadline_ms: 1500,
        };
        assert_eq!(CallPrologue::decode(&prologue.encode()), prologue);
    }

    #[test]
    fn method_ids_are_stable() {
        // Pinned: a change here is a wire format break.
        assert_eq!(method_id(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(method_id("interop.Echo/say"), 0x2144_947f_5c60_b34d);
    }

    #[test]
    fn frame_io_round_trips() {
        let mut buf = Vec::new();
        let header = FrameHeader {
            kind: FrameKind::MESSAGE,
            flags: 0,
            status: 0,
            call_id: 3,
            length: 0, // overwritten by write_frame
        };
        write_frame(&mut buf, header, b"hello").unwrap();
        let (read_header, payload) = read_frame(&mut buf.as_slice(), MAX_FRAME_LEN).unwrap();
        assert_eq!(read_header.kind, FrameKind::MESSAGE);
        assert_eq!(read_header.length, 5);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn oversized_frames_are_rejected() {
        let header = FrameHeader {
            kind: FrameKind::MESSAGE,
            flags: 0,
            status: 0,
            call_id: 1,
            length: MAX_FRAME_LEN + 1,
        };
        let bytes = header.encode();
        let err = read_frame(&mut bytes.as_slice(), MAX_FRAME_LEN).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn codes_match_grpc_numbering() {
        assert_eq!(Code::OK.0, 0);
        assert_eq!(Code::DEADLINE_EXCEEDED.0, 4);
        assert_eq!(Code::UNIMPLEMENTED.0, 12);
        assert_eq!(Code::UNAUTHENTICATED.0, 16);
        assert_eq!(Code(99).name(), None);
    }
}
