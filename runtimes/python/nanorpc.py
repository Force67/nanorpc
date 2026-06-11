"""Python client for the nanorpc protocol.

Mirrors the Rust runtime (`crates/nanorpc`): one TCP connection, calls
multiplexed by id, a background reader thread routing frames to waiting
calls. The wire format is specified in `docs/protocol.md`; this module
must stay byte-compatible with `crates/nanorpc-wire`.

Messages are nanobuf-generated classes (anything with `encode()` and a
`decode()` classmethod), so generated code plugs in directly:

    say = nanorpc.Method("interop.Echo/say", EchoRequest, EchoResponse)
    client = nanorpc.Client.connect("127.0.0.1", port)
    reply = client.call(say, EchoRequest(text="hi"), deadline=1.0)
"""

import queue
import socket
import struct
import threading

PREFACE = b"nanorpc\x01"
MAX_FRAME_LEN = 64 * 1024 * 1024

_HEADER = struct.Struct("<BBHIII")
_PROLOGUE = struct.Struct("<QI")

KIND_CALL = 0
KIND_MESSAGE = 1
KIND_CLOSE = 2
KIND_CANCEL = 3
KIND_PING = 4
KIND_PONG = 5

OK = 0
CANCELLED = 1
UNKNOWN = 2
INVALID_ARGUMENT = 3
DEADLINE_EXCEEDED = 4
NOT_FOUND = 5
ALREADY_EXISTS = 6
PERMISSION_DENIED = 7
RESOURCE_EXHAUSTED = 8
FAILED_PRECONDITION = 9
ABORTED = 10
OUT_OF_RANGE = 11
UNIMPLEMENTED = 12
INTERNAL = 13
UNAVAILABLE = 14
DATA_LOSS = 15
UNAUTHENTICATED = 16

_CODE_NAMES = {
    0: "ok",
    1: "cancelled",
    2: "unknown",
    3: "invalid_argument",
    4: "deadline_exceeded",
    5: "not_found",
    6: "already_exists",
    7: "permission_denied",
    8: "resource_exhausted",
    9: "failed_precondition",
    10: "aborted",
    11: "out_of_range",
    12: "unimplemented",
    13: "internal",
    14: "unavailable",
    15: "data_loss",
    16: "unauthenticated",
}


def method_id(path: str) -> int:
    """64-bit FNV-1a of the method path; must match `nanorpc_wire`."""
    value = 0xCBF29CE484222325
    for byte in path.encode("utf-8"):
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


class RpcError(Exception):
    """A call that did not end with OK: server status, local deadline, or
    a dead connection. `code` uses the shared (gRPC-compatible) numbers."""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        name = _CODE_NAMES.get(code, f"code({code})")
        super().__init__(f"{name}: {message}" if message else name)


class Method:
    """A unary method: path plus request/response message classes."""

    def __init__(self, path: str, request_type, response_type):
        self.path = path
        self.id = method_id(path)
        self.request_type = request_type
        self.response_type = response_type


class StreamMethod(Method):
    """A server-streaming method."""


class _Call:
    __slots__ = ("events",)

    def __init__(self):
        self.events = queue.Queue()


class Streaming:
    """Messages of one server-streaming call, in arrival order. Iterate to
    consume; `cancel()` (or an early `close`) tells the server to stop."""

    def __init__(self, client, call, call_id, response_type, expires):
        self._client = client
        self._call = call
        self._call_id = call_id
        self._response_type = response_type
        self._expires = expires
        self._finished = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._finished:
            raise StopIteration
        kind, payload = self._client._wait(self._call, self._expires, self._call_id)
        if kind == KIND_MESSAGE:
            return self._response_type.decode(payload)
        status, message = payload
        self._finished = True
        if status == OK:
            raise StopIteration
        raise RpcError(status, message)

    def cancel(self):
        if not self._finished:
            self._finished = True
            self._client._cancel(self._call_id)


class Client:
    """A connection to one nanorpc server, usable from multiple threads."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._write_lock = threading.Lock()
        self._calls_lock = threading.Lock()
        self._calls = {}
        self._next_id = 1
        reader = threading.Thread(target=self._read_loop, daemon=True)
        reader.start()

    @classmethod
    def connect(cls, host: str, port: int) -> "Client":
        sock = socket.create_connection((host, port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.sendall(PREFACE)
        if _read_exact(sock, len(PREFACE)) != PREFACE:
            sock.close()
            raise RpcError(UNAVAILABLE, "peer is not a nanorpc/1 server")
        return cls(sock)

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def call(self, method: Method, request, deadline=None):
        """Unary call. `deadline` is seconds; it bounds the local wait and
        travels to the server, which stops work once it expires."""
        call, call_id, expires = self._start(method, request, deadline)
        response = None
        while True:
            kind, payload = self._wait(call, expires, call_id)
            if kind == KIND_MESSAGE:
                if response is not None:
                    self._cancel(call_id)
                    raise RpcError(INTERNAL, "second message on a unary call")
                response = payload
                continue
            status, message = payload
            with self._calls_lock:
                self._calls.pop(call_id, None)
            if status != OK:
                raise RpcError(status, message)
            if response is None:
                raise RpcError(INTERNAL, "CLOSE(ok) without a response message")
            return method.response_type.decode(response)

    def server_stream(self, method: StreamMethod, request, deadline=None) -> Streaming:
        call, call_id, expires = self._start(method, request, deadline)
        return Streaming(self, call, call_id, method.response_type, expires)

    def _start(self, method, request, deadline):
        import time

        deadline_ms = 0
        expires = None
        if deadline is not None:
            deadline_ms = max(1, min(int(deadline * 1000), 0xFFFFFFFF))
            expires = time.monotonic() + deadline
        payload = _PROLOGUE.pack(method.id, deadline_ms) + request.encode()
        call = _Call()
        with self._calls_lock:
            call_id = self._next_id
            self._next_id += 1
            self._calls[call_id] = call
        try:
            self._send(KIND_CALL, call_id, payload)
        except OSError as err:
            with self._calls_lock:
                self._calls.pop(call_id, None)
            raise RpcError(UNAVAILABLE, f"send failed: {err}") from err
        return call, call_id, expires

    def _wait(self, call, expires, call_id):
        import time

        timeout = None
        if expires is not None:
            timeout = max(0.0, expires - time.monotonic())
        try:
            return call.events.get(timeout=timeout)
        except queue.Empty:
            self._cancel(call_id)
            raise RpcError(DEADLINE_EXCEEDED, "deadline exceeded") from None

    def _cancel(self, call_id):
        with self._calls_lock:
            self._calls.pop(call_id, None)
        try:
            self._send(KIND_CANCEL, call_id, b"")
        except OSError:
            pass

    def _send(self, kind, call_id, payload, status=0):
        frame = _HEADER.pack(kind, 0, status, call_id, len(payload), 0) + payload
        with self._write_lock:
            self._sock.sendall(frame)

    def _read_loop(self):
        try:
            while True:
                header = _read_exact(self._sock, _HEADER.size)
                kind, _flags, status, call_id, length, _ = _HEADER.unpack(header)
                if length > MAX_FRAME_LEN:
                    break
                payload = _read_exact(self._sock, length)
                if kind == KIND_MESSAGE:
                    with self._calls_lock:
                        call = self._calls.get(call_id)
                    if call is not None:
                        call.events.put((KIND_MESSAGE, payload))
                elif kind == KIND_CLOSE:
                    with self._calls_lock:
                        call = self._calls.pop(call_id, None)
                    if call is not None:
                        detail = payload.decode("utf-8", errors="replace")
                        call.events.put((KIND_CLOSE, (status, detail)))
                elif kind == KIND_PING:
                    self._send(KIND_PONG, 0, payload)
                elif kind == KIND_PONG:
                    pass
                else:
                    break
        except OSError:
            pass
        # Connection over: fail every waiting call immediately.
        with self._calls_lock:
            calls, self._calls = self._calls, {}
        for call in calls.values():
            call.events.put((KIND_CLOSE, (UNAVAILABLE, "connection closed mid-call")))


def _read_exact(sock: socket.socket, count: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            raise OSError("connection closed")
        chunks += chunk
    return bytes(chunks)
