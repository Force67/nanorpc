# The nanorpc wire protocol, version 1

nanorpc runs over any reliable byte stream (TCP in the shipped runtimes).
Everything after the preface is a 16-byte frame header followed by a
payload. All integers are little-endian, matching nanobuf.

## Preface

Each peer sends 8 bytes immediately after connecting, before anything
else:

```
6e 61 6e 6f 72 70 63 01      "nanorpc" + version 1
```

A peer that reads anything else closes the connection. The version byte
is the only negotiation: there are no feature flags to probe.

## Frame header

```
offset  size  field
0       1     kind
1       1     flags     (must be 0 in version 1)
2       2     status    (CLOSE only; 0 elsewhere)
4       4     call id   (0 on connection-level frames)
8       4     payload length
12      4     reserved  (must be 0)
```

Frames are atomic: a sender never interleaves the bytes of two frames.
Frames of *different calls* interleave freely; that is the multiplexing.
A payload longer than 64 MiB is a protocol violation.

## Frame kinds

| kind | name    | direction | payload |
|------|---------|-----------|---------|
| 0    | CALL    | client to server | 12-byte prologue, then the request message |
| 1    | MESSAGE | server to client | one response message |
| 2    | CLOSE   | server to client | UTF-8 detail string; `status` holds the code |
| 3    | CANCEL  | client to server | empty |
| 4    | PING    | either    | 8 opaque bytes |
| 5    | PONG    | either    | the PING payload, echoed |

An unknown kind, a nonzero flag, or a MESSAGE sent by a client is a
protocol violation; the receiver closes the connection. Version 2 can
claim any of these without being misread by a version 1 peer.

## Calls

The client picks a nonzero call id, unique among its in-flight calls
(the runtimes count up from 1), and sends CALL. The payload starts with
the prologue:

```
offset  size  field
0       8     method id
8       4     deadline in ms   (0 = unbounded)
```

The method id is the 64-bit FNV-1a hash of the method path, e.g.
`interop.Echo/say`. Servers reject colliding registrations at startup,
so a collision is a build failure, never a misrouted call. Routing is
one integer lookup; no string leaves the process.

The deadline is *relative*: milliseconds of patience remaining, not a
timestamp, so peers never need synchronized clocks. A server that
receives an already-expired call answers CLOSE(DEADLINE_EXCEEDED)
without dispatching. Forwarding services subtract time spent before
calling onward.

The server answers with zero or more MESSAGE frames, then exactly one
CLOSE. That is the whole lifecycle:

* unary call: `MESSAGE` + `CLOSE(0)`, or just `CLOSE(error)`
* server stream: any number of `MESSAGE` + one `CLOSE`

There is no second status channel, no trailers, no special empty-body
encoding. If `status` is 0, the call succeeded; the CLOSE payload is
empty. Otherwise the payload is a human-readable detail string.

CANCEL asks the server to stop a call. It is advisory and races with
completion by design; the server still sends CLOSE (typically with
status 1, CANCELLED), and the client ignores frames for ids it no
longer tracks.

## Status codes

Codes are numerically identical to gRPC's, 0 (`OK`) through 16
(`UNAUTHENTICATED`), so migrating services keep their alerting and
retry policies. Codes are open: unknown values are carried, not
rejected.

## Messages

A MESSAGE payload (and the request after the CALL prologue) is exactly
one nanobuf-encoded message, nothing more. The frame header already
carries the length, so a proxy can route, duplicate, or buffer calls
without decoding them, and a recipient gets nanobuf's zero-copy reads
straight off the receive buffer.

## What version 1 leaves out

Client streaming and bidirectional streaming (the MESSAGE kind is
reserved client-to-server for exactly this), TLS (terminate it in the
proxy of your choice, or below the stream), and compression. Absences
are spec'd so version 2 can add them without a flag day.
