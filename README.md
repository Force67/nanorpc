# nanorpc

RPC for [nanobuf](https://github.com/Force67/nanobuf) messages. Calls are
multiplexed over one TCP connection behind a 16-byte frame header, and the
whole wire format is specified on a single page in
[docs/protocol.md](docs/protocol.md).

Services are plain values. There is no service codegen and no macro
ceremony beyond declaring the message types:

```rust
use nanorpc::{Client, Method, Router, Server, messages};

messages!(EchoRequest, EchoResponse);

const SAY: Method<EchoRequest, EchoResponse> = Method::new("interop.Echo/say");

// Server
let router = Router::new().unary(SAY, |_ctx, req: EchoRequest| {
    Ok(EchoResponse { text: req.text })
});
Server::new(router).serve(listener)?;

// Client (any thread, calls run concurrently over one connection)
let client = Client::connect(addr)?;
let reply = client.call(SAY, &request, Some(Duration::from_secs(1)))?;
```

## Why not gRPC

gRPC assumes HTTP/2, and HTTP/2 is not small. A nanorpc connection is a
plain TCP stream with a small frame header instead. Status codes keep
gRPC's exact numbers, so a service can move over without rewiring its
dashboards or retry rules.

The runtime is blocking and capped, so a flood of connections or calls is
refused rather than left to sink the host. Server streaming works today,
client and bidirectional do not. There is no TLS, so terminate it
underneath.

## Repository layout

| path | what it does |
|---|---|
| `crates/nanorpc-wire` | the frame layer: headers, statuses, method ids |
| `crates/nanorpc` | Rust runtime: `Client`, `Server`, `Router` |
| `runtimes/python` | Python client speaking the same bytes |
| `interop/` | shared echo service, reference server, cross-language tests |
| `docs/protocol.md` | the wire protocol, normative |

The build expects the nanobuf repo as a sibling checkout at `../nanobuf`,
where the message types and the schema compiler live.

## Testing

```sh
cargo test                  # wire layer + Rust client/server loopback
scripts/test-all.sh         # plus the Python-client-vs-Rust-server suite
```

The interop suite is the contract. A client runtime in any language has to
show the reference server the same statuses and ordering, and react to
deadlines and cancellation the same way.
