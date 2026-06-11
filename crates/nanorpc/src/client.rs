//! Client half of the runtime: one TCP connection multiplexing any number
//! of concurrent calls. One background thread reads frames and routes them
//! to waiting calls by id; senders share the socket behind a mutex, one
//! frame per lock hold.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nanorpc_wire::{
    CallPrologue, Code, FrameHeader, FrameKind, PREFACE, Status, read_frame, write_frame,
};

use crate::{Message, Method, RpcError, StreamMethod};

enum Event {
    Message(Vec<u8>),
    Close(Status),
}

struct Inner {
    writer: Mutex<TcpStream>,
    calls: Mutex<HashMap<u32, Sender<Event>>>,
    pings: Mutex<HashMap<u64, Sender<()>>>,
    next_call: AtomicU32,
    next_ping: AtomicU64,
    max_message: u32,
}

impl Inner {
    fn send_frame(&self, header: FrameHeader, payload: &[u8]) -> io::Result<()> {
        write_frame(&mut *self.writer.lock().unwrap(), header, payload)
    }

    fn cancel(&self, call_id: u32) {
        self.calls.lock().unwrap().remove(&call_id);
        let header = FrameHeader {
            kind: FrameKind::CANCEL,
            flags: 0,
            status: 0,
            call_id,
            length: 0,
        };
        // Best-effort: if the connection is gone the call is over anyway.
        let _ = self.send_frame(header, &[]);
    }
}

/// A connection to one nanorpc server. Calls may be issued from any number
/// of threads; they share the connection and complete independently.
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Client> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.write_all(&PREFACE)?;
        let mut preface = [0u8; PREFACE.len()];
        stream.read_exact(&mut preface)?;
        if preface != PREFACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer is not a nanorpc/1 server",
            ));
        }

        let inner = Arc::new(Inner {
            writer: Mutex::new(stream.try_clone()?),
            calls: Mutex::default(),
            pings: Mutex::default(),
            next_call: AtomicU32::new(1),
            next_ping: AtomicU64::new(1),
            max_message: nanorpc_wire::MAX_FRAME_LEN,
        });
        let reader_inner = Arc::clone(&inner);
        std::thread::spawn(move || read_loop(&reader_inner, stream));
        Ok(Client { inner })
    }

    /// Issues a unary call and waits for the response. `deadline` bounds
    /// the wait locally *and* travels in the CALL frame, so the server
    /// stops working on a call nobody is waiting for.
    pub fn call<Req: Message, Res: Message>(
        &self,
        method: Method<Req, Res>,
        request: &Req,
        deadline: Option<Duration>,
    ) -> Result<Res, RpcError> {
        let (call_id, rx) = self.start(method.id, request, deadline)?;
        let expires = deadline.map(|d| Instant::now() + d);

        let mut response: Option<Vec<u8>> = None;
        loop {
            match recv(&rx, expires) {
                Ok(Event::Message(bytes)) => {
                    if response.replace(bytes).is_some() {
                        self.inner.cancel(call_id);
                        return Err(RpcError::Protocol(
                            "second message on a unary call".to_string(),
                        ));
                    }
                }
                Ok(Event::Close(status)) => {
                    self.inner.calls.lock().unwrap().remove(&call_id);
                    if !status.is_ok() {
                        return Err(RpcError::Status(status));
                    }
                    let Some(bytes) = response else {
                        return Err(RpcError::Protocol(
                            "CLOSE(ok) without a response message".to_string(),
                        ));
                    };
                    return Res::from_bytes(&bytes).map_err(RpcError::Decode);
                }
                Err(timeout) => {
                    self.inner.cancel(call_id);
                    return Err(timeout);
                }
            }
        }
    }

    /// Issues a server-streaming call. The returned [`Streaming`] yields
    /// messages as they arrive and reports how the call ended; dropping it
    /// early cancels the call.
    pub fn server_stream<Req: Message, Res: Message>(
        &self,
        method: StreamMethod<Req, Res>,
        request: &Req,
        deadline: Option<Duration>,
    ) -> Result<Streaming<Res>, RpcError> {
        let (call_id, rx) = self.start(method.id, request, deadline)?;
        Ok(Streaming {
            inner: Arc::clone(&self.inner),
            rx,
            call_id,
            expires: deadline.map(|d| Instant::now() + d),
            finished: false,
            types: PhantomData,
        })
    }

    /// Round-trip probe over the connection's PING frame.
    pub fn ping(&self, timeout: Duration) -> Result<Duration, RpcError> {
        let nonce = self.inner.next_ping.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        self.inner.pings.lock().unwrap().insert(nonce, tx);
        let header = FrameHeader {
            kind: FrameKind::PING,
            flags: 0,
            status: 0,
            call_id: 0,
            length: 0,
        };
        let started = Instant::now();
        let sent = self.inner.send_frame(header, &nonce.to_le_bytes());
        let result = match sent {
            Ok(()) => rx
                .recv_timeout(timeout)
                .map(|()| started.elapsed())
                .map_err(|_| {
                    RpcError::Status(Status::new(Code::DEADLINE_EXCEEDED, "ping timed out"))
                }),
            Err(err) => Err(RpcError::Transport(err)),
        };
        self.inner.pings.lock().unwrap().remove(&nonce);
        result
    }

    fn start(
        &self,
        method: u64,
        request: &impl Message,
        deadline: Option<Duration>,
    ) -> Result<(u32, Receiver<Event>), RpcError> {
        let body = request.to_bytes();
        let prologue = CallPrologue {
            method,
            deadline_ms: deadline
                .map(|d| d.as_millis().min(u32::MAX as u128) as u32)
                .unwrap_or(0),
        };
        let mut payload = Vec::with_capacity(prologue.encode().len() + body.len());
        payload.extend_from_slice(&prologue.encode());
        payload.extend_from_slice(&body);
        if payload.len() > self.inner.max_message as usize {
            return Err(RpcError::Status(Status::new(
                Code::RESOURCE_EXHAUSTED,
                format!("request of {} bytes exceeds the frame limit", payload.len()),
            )));
        }

        let call_id = self.inner.next_call.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        self.inner.calls.lock().unwrap().insert(call_id, tx);
        let header = FrameHeader {
            kind: FrameKind::CALL,
            flags: 0,
            status: 0,
            call_id,
            length: 0,
        };
        if let Err(err) = self.inner.send_frame(header, &payload) {
            self.inner.calls.lock().unwrap().remove(&call_id);
            return Err(RpcError::Transport(err));
        }
        Ok((call_id, rx))
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Unblocks the reader thread; in-flight calls end with UNAVAILABLE.
        if let Ok(stream) = self.inner.writer.lock() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

/// Messages of one server-streaming call, in arrival order.
pub struct Streaming<Res> {
    inner: Arc<Inner>,
    rx: Receiver<Event>,
    call_id: u32,
    expires: Option<Instant>,
    finished: bool,
    types: PhantomData<fn() -> Res>,
}

impl<Res: Message> Streaming<Res> {
    /// Stops the stream early; the server sees a CANCEL frame.
    pub fn cancel(mut self) {
        self.finish_early();
    }

    fn finish_early(&mut self) {
        if !self.finished {
            self.finished = true;
            self.inner.cancel(self.call_id);
        }
    }
}

impl<Res: Message> Iterator for Streaming<Res> {
    type Item = Result<Res, RpcError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match recv(&self.rx, self.expires) {
            Ok(Event::Message(bytes)) => match Res::from_bytes(&bytes) {
                Ok(item) => Some(Ok(item)),
                Err(err) => {
                    self.finish_early();
                    Some(Err(RpcError::Decode(err)))
                }
            },
            Ok(Event::Close(status)) => {
                self.finished = true;
                self.inner.calls.lock().unwrap().remove(&self.call_id);
                if status.is_ok() {
                    None
                } else {
                    Some(Err(RpcError::Status(status)))
                }
            }
            Err(timeout) => {
                self.finish_early();
                Some(Err(timeout))
            }
        }
    }
}

impl<Res> Drop for Streaming<Res> {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            self.inner.cancel(self.call_id);
        }
    }
}

/// Waits for the next event, honoring an absolute expiry. Disconnection
/// and expiry both surface as `RpcError`.
fn recv(rx: &Receiver<Event>, expires: Option<Instant>) -> Result<Event, RpcError> {
    let result = match expires {
        None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        Some(at) => rx.recv_timeout(at.saturating_duration_since(Instant::now())),
    };
    result.map_err(|err| match err {
        RecvTimeoutError::Timeout => {
            RpcError::Status(Status::new(Code::DEADLINE_EXCEEDED, "deadline exceeded"))
        }
        RecvTimeoutError::Disconnected => {
            RpcError::Status(Status::new(Code::UNAVAILABLE, "connection closed mid-call"))
        }
    })
}

fn read_loop(inner: &Inner, mut stream: TcpStream) {
    while let Ok((header, payload)) = read_frame(&mut stream, inner.max_message) {
        match header.kind {
            FrameKind::MESSAGE => {
                if let Some(tx) = inner.calls.lock().unwrap().get(&header.call_id) {
                    let _ = tx.send(Event::Message(payload));
                }
            }
            FrameKind::CLOSE => {
                // Remove first so late frames for this id fall on the floor.
                if let Some(tx) = inner.calls.lock().unwrap().remove(&header.call_id) {
                    let status = Status::new(
                        nanorpc_wire::Code(header.status),
                        String::from_utf8_lossy(&payload).into_owned(),
                    );
                    let _ = tx.send(Event::Close(status));
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
                if inner.send_frame(pong, &payload).is_err() {
                    break;
                }
            }
            FrameKind::PONG => {
                if payload.len() == 8 {
                    let nonce = u64::from_le_bytes(payload.try_into().unwrap());
                    if let Some(tx) = inner.pings.lock().unwrap().remove(&nonce) {
                        let _ = tx.send(());
                    }
                }
            }
            _ => break,
        }
    }
    // Connection over: every waiting call learns immediately, because the
    // channel senders drop here and `recv` reports Disconnected.
    inner.calls.lock().unwrap().clear();
    inner.pings.lock().unwrap().clear();
}
