//! Server half of the runtime: a [`Router`] of registered methods served
//! over TCP, one thread per connection and per in-flight call.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nanorpc_wire::{
    CALL_PROLOGUE_LEN, CallPrologue, Code, FrameHeader, FrameKind, PREFACE, Status, read_frame,
    write_frame,
};

use crate::{Message, Method, StreamMethod};

/// What a handler knows about its call beyond the request itself.
pub struct Context {
    deadline: Option<Instant>,
    cancelled: Arc<AtomicBool>,
    peer: SocketAddr,
}

impl Context {
    /// Instant after which the client has given up. Derived from the
    /// hop-relative deadline in the CALL frame.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Time left to produce an answer; `None` means unbounded.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    /// True once the client cancelled or the deadline passed. Long
    /// handlers and streams should poll this and bail out.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
            || self.deadline.is_some_and(|at| Instant::now() >= at)
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

/// Typed outgoing message stream handed to server-streaming handlers.
pub struct Sink<'a, Res> {
    raw: &'a mut dyn FnMut(Vec<u8>) -> Result<(), Status>,
    types: PhantomData<fn(&Res)>,
}

impl<Res: Message> Sink<'_, Res> {
    /// Sends one message. Fails once the client cancelled or went away;
    /// handlers should return the error as-is.
    pub fn send(&mut self, item: &Res) -> Result<(), Status> {
        (self.raw)(item.to_bytes())
    }
}

type UnaryFn = dyn Fn(&Context, &[u8]) -> Result<Vec<u8>, Status> + Send + Sync;
type StreamFn = dyn Fn(&Context, &[u8], &mut dyn FnMut(Vec<u8>) -> Result<(), Status>) -> Result<(), Status>
    + Send
    + Sync;

enum Handler {
    Unary(Box<UnaryFn>),
    Stream(Box<StreamFn>),
}

/// Maps method ids to handlers. Built once, then shared by every
/// connection; registration is the only place paths exist, so a hash
/// collision between two registered methods panics here instead of
/// misrouting calls at runtime.
#[derive(Default)]
pub struct Router {
    handlers: HashMap<u64, (&'static str, Handler)>,
}

impl Router {
    pub fn new() -> Router {
        Router::default()
    }

    pub fn unary<Req: Message, Res: Message>(
        mut self,
        method: Method<Req, Res>,
        handler: impl Fn(&Context, Req) -> Result<Res, Status> + Send + Sync + 'static,
    ) -> Router {
        let erased = Handler::Unary(Box::new(move |ctx, bytes| {
            let request = Req::from_bytes(bytes)
                .map_err(|_| Status::new(Code::INVALID_ARGUMENT, "request did not decode"))?;
            Ok(handler(ctx, request)?.to_bytes())
        }));
        self.insert(method.id, method.path, erased);
        self
    }

    pub fn stream<Req: Message, Res: Message>(
        mut self,
        method: StreamMethod<Req, Res>,
        handler: impl Fn(&Context, Req, &mut Sink<'_, Res>) -> Result<(), Status>
        + Send
        + Sync
        + 'static,
    ) -> Router {
        let erased = Handler::Stream(Box::new(move |ctx, bytes, raw| {
            let request = Req::from_bytes(bytes)
                .map_err(|_| Status::new(Code::INVALID_ARGUMENT, "request did not decode"))?;
            let mut sink = Sink {
                raw,
                types: PhantomData,
            };
            handler(ctx, request, &mut sink)
        }));
        self.insert(method.id, method.path, erased);
        self
    }

    fn insert(&mut self, id: u64, path: &'static str, handler: Handler) {
        if let Some((existing, _)) = self.handlers.get(&id) {
            if *existing == path {
                panic!("method `{path}` registered twice");
            }
            panic!("method id collision: `{path}` and `{existing}` share {id:#018x}");
        }
        self.handlers.insert(id, (path, handler));
    }
}

/// Serves a [`Router`] on a TCP listener.
pub struct Server {
    router: Arc<Router>,
    max_message: u32,
}

impl Server {
    pub fn new(router: Router) -> Server {
        Server {
            router: Arc::new(router),
            max_message: nanorpc_wire::MAX_FRAME_LEN,
        }
    }

    /// Lowers the per-frame payload limit (default 64 MiB).
    pub fn max_message(mut self, bytes: u32) -> Server {
        self.max_message = bytes.min(nanorpc_wire::MAX_FRAME_LEN);
        self
    }

    /// Accept loop. Each connection gets a thread; each in-flight call
    /// gets a thread. Returns only when the listener fails.
    pub fn serve(&self, listener: TcpListener) -> io::Result<()> {
        loop {
            let (stream, peer) = listener.accept()?;
            let router = Arc::clone(&self.router);
            let max_message = self.max_message;
            std::thread::spawn(move || {
                // A failed connection takes only itself down.
                let _ = serve_connection(&router, stream, peer, max_message);
            });
        }
    }
}

fn serve_connection(
    router: &Arc<Router>,
    mut stream: TcpStream,
    peer: SocketAddr,
    max_message: u32,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.write_all(&PREFACE)?;
    let mut preface = [0u8; PREFACE.len()];
    stream.read_exact(&mut preface)?;
    if preface != PREFACE {
        stream.shutdown(Shutdown::Both)?;
        return Ok(());
    }

    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    let cancels: Arc<Mutex<HashMap<u32, Arc<AtomicBool>>>> = Arc::default();

    loop {
        let (header, payload) = read_frame(&mut stream, max_message)?;
        match header.kind {
            FrameKind::CALL => {
                if payload.len() < CALL_PROLOGUE_LEN {
                    break; // protocol violation: drop the connection
                }
                let prologue =
                    CallPrologue::decode(&payload[..CALL_PROLOGUE_LEN].try_into().unwrap());
                let cancelled = Arc::new(AtomicBool::new(false));
                cancels
                    .lock()
                    .unwrap()
                    .insert(header.call_id, Arc::clone(&cancelled));
                let router = Arc::clone(router);
                let writer = Arc::clone(&writer);
                let cancels = Arc::clone(&cancels);
                let call_id = header.call_id;
                std::thread::spawn(move || {
                    run_call(
                        &router,
                        &writer,
                        Context {
                            deadline: (prologue.deadline_ms > 0).then(|| {
                                Instant::now() + Duration::from_millis(prologue.deadline_ms.into())
                            }),
                            cancelled,
                            peer,
                        },
                        call_id,
                        prologue.method,
                        &payload[CALL_PROLOGUE_LEN..],
                    );
                    cancels.lock().unwrap().remove(&call_id);
                });
            }
            FrameKind::CANCEL => {
                if let Some(flag) = cancels.lock().unwrap().get(&header.call_id) {
                    flag.store(true, Ordering::Relaxed);
                }
            }
            FrameKind::PING => {
                let pong = FrameHeader {
                    kind: FrameKind::PONG,
                    flags: 0,
                    status: 0,
                    call_id: 0,
                    length: 0,
                };
                write_frame(&mut *writer.lock().unwrap(), pong, &payload)?;
            }
            FrameKind::PONG => {}
            // MESSAGE from a client (client streaming) is not part of
            // protocol version 1; anything else is unknown.
            _ => break,
        }
    }
    stream.shutdown(Shutdown::Both)?;
    Ok(())
}

fn run_call(
    router: &Router,
    writer: &Mutex<TcpStream>,
    ctx: Context,
    call_id: u32,
    method: u64,
    request: &[u8],
) {
    let close = |status: Status| {
        let header = FrameHeader {
            kind: FrameKind::CLOSE,
            flags: 0,
            status: status.code.0,
            call_id,
            length: 0,
        };
        // The client may already be gone; nothing left to tell anyone.
        let _ = write_frame(
            &mut *writer.lock().unwrap(),
            header,
            status.message.as_bytes(),
        );
    };

    let Some((_, handler)) = router.handlers.get(&method) else {
        close(Status::new(
            Code::UNIMPLEMENTED,
            format!("unknown method {method:#018x}"),
        ));
        return;
    };
    if ctx.is_cancelled() {
        // The budget was spent in transit; do not run the handler at all.
        close(Status::new(
            Code::DEADLINE_EXCEEDED,
            "deadline expired before dispatch",
        ));
        return;
    }

    match handler {
        Handler::Unary(handler) => match handler(&ctx, request) {
            Ok(response) => {
                let header = FrameHeader {
                    kind: FrameKind::MESSAGE,
                    flags: 0,
                    status: 0,
                    call_id,
                    length: 0,
                };
                let _ = write_frame(&mut *writer.lock().unwrap(), header, &response);
                close(Status::ok());
            }
            Err(status) => close(status),
        },
        Handler::Stream(handler) => {
            let cancelled = &ctx.cancelled;
            let deadline = ctx.deadline;
            let mut raw = |bytes: Vec<u8>| -> Result<(), Status> {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(Status::new(Code::CANCELLED, "client cancelled"));
                }
                if deadline.is_some_and(|at| Instant::now() >= at) {
                    return Err(Status::new(Code::DEADLINE_EXCEEDED, "deadline expired"));
                }
                let header = FrameHeader {
                    kind: FrameKind::MESSAGE,
                    flags: 0,
                    status: 0,
                    call_id,
                    length: 0,
                };
                write_frame(&mut *writer.lock().unwrap(), header, &bytes)
                    .map_err(|_| Status::new(Code::UNAVAILABLE, "client connection lost"))
            };
            match handler(&ctx, request, &mut raw) {
                Ok(()) => close(Status::ok()),
                Err(status) => close(status),
            }
        }
    }
}
