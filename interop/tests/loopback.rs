//! Rust client against the Rust reference server, over real sockets.

use std::net::TcpListener;
use std::time::Duration;

use nanorpc::{Client, Code, Method, RpcError, Server};
use nanorpc_interop::{
    COUNT, CountRequest, EchoRequest, EchoResponse, FAIL, FailRequest, SAY, SLEEP, SleepRequest,
};

/// Starts the reference server on an ephemeral port, returns a client.
fn connect() -> Client {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let _ = Server::new(nanorpc_interop::router()).serve(listener);
    });
    Client::connect(addr).unwrap()
}

#[test]
fn unary_round_trip() {
    let client = connect();
    let reply = client
        .call(
            SAY,
            &EchoRequest {
                text: "hello, wire".to_string(),
                shout: false,
            },
            None,
        )
        .unwrap();
    assert_eq!(reply.text, "hello, wire");

    let loud = client
        .call(
            SAY,
            &EchoRequest {
                text: "hello".to_string(),
                shout: true,
            },
            None,
        )
        .unwrap();
    assert_eq!(loud.text, "HELLO");
}

#[test]
fn concurrent_calls_share_one_connection() {
    let client = std::sync::Arc::new(connect());
    let mut workers = Vec::new();
    for i in 0..16 {
        let client = std::sync::Arc::clone(&client);
        workers.push(std::thread::spawn(move || {
            let text = format!("call {i}");
            let reply = client
                .call(
                    SAY,
                    &EchoRequest {
                        text: text.clone(),
                        shout: false,
                    },
                    Some(Duration::from_secs(5)),
                )
                .unwrap();
            assert_eq!(reply.text, text);
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn server_stream_delivers_in_order() {
    let client = connect();
    let stream = client
        .server_stream(
            COUNT,
            &CountRequest {
                up_to: 100,
                fail_after: None,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    let values: Vec<u32> = stream.map(|item| item.unwrap().value).collect();
    assert_eq!(values, (1..=100).collect::<Vec<u32>>());
}

#[test]
fn mid_stream_failure_carries_the_status() {
    let client = connect();
    let mut stream = client
        .server_stream(
            COUNT,
            &CountRequest {
                up_to: 10,
                fail_after: Some(3),
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    for expected in 1..=3 {
        assert_eq!(stream.next().unwrap().unwrap().value, expected);
    }
    match stream.next().unwrap() {
        Err(RpcError::Status(status)) => {
            assert_eq!(status.code, Code::ABORTED);
            assert_eq!(status.message, "failed mid-stream as requested");
        }
        other => panic!("expected ABORTED, got {other:?}"),
    }
    assert!(stream.next().is_none());
}

#[test]
fn error_statuses_survive_the_wire() {
    let client = connect();
    let err = client
        .call(
            FAIL,
            &FailRequest {
                code: Code::PERMISSION_DENIED.0,
                detail: "no such key".to_string(),
            },
            None,
        )
        .unwrap_err();
    match err {
        RpcError::Status(status) => {
            assert_eq!(status.code, Code::PERMISSION_DENIED);
            assert_eq!(status.message, "no such key");
        }
        other => panic!("expected a status, got {other:?}"),
    }
}

#[test]
fn unknown_methods_are_unimplemented() {
    const MISSING: Method<EchoRequest, EchoResponse> = Method::new("interop.Echo/no_such");
    let client = connect();
    let err = client
        .call(
            MISSING,
            &EchoRequest {
                text: String::new(),
                shout: false,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap_err();
    assert_eq!(err.code(), Some(Code::UNIMPLEMENTED));
}

#[test]
fn deadlines_cut_calls_short() {
    let client = connect();
    let started = std::time::Instant::now();
    let err = client
        .call(
            SLEEP,
            &SleepRequest { ms: 5_000 },
            Some(Duration::from_millis(80)),
        )
        .unwrap_err();
    assert_eq!(err.code(), Some(Code::DEADLINE_EXCEEDED));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "deadline did not cut the wait"
    );

    // The connection is still healthy for the next call.
    let reply = client
        .call(
            SAY,
            &EchoRequest {
                text: "still alive".to_string(),
                shout: false,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(reply.text, "still alive");
}

#[test]
fn dropping_a_stream_cancels_it() {
    let client = connect();
    let mut stream = client
        .server_stream(
            COUNT,
            &CountRequest {
                up_to: u32::MAX, // would stream forever
                fail_after: None,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(stream.next().unwrap().unwrap().value, 1);
    drop(stream); // sends CANCEL

    // The connection keeps working after the cancelled call.
    let reply = client
        .call(
            SAY,
            &EchoRequest {
                text: "after cancel".to_string(),
                shout: false,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(reply.text, "after cancel");
}

#[test]
fn large_payloads_round_trip() {
    let client = connect();
    let text = "x".repeat(1 << 20);
    let reply = client
        .call(
            SAY,
            &EchoRequest {
                text: text.clone(),
                shout: false,
            },
            Some(Duration::from_secs(10)),
        )
        .unwrap();
    assert_eq!(reply.text.len(), text.len());
}

#[test]
fn ping_measures_the_connection() {
    let client = connect();
    let rtt = client.ping(Duration::from_secs(5)).unwrap();
    assert!(rtt < Duration::from_secs(1));
}

#[test]
fn calls_past_the_cap_are_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let _ = Server::new(nanorpc_interop::router())
            .max_concurrent_calls(1)
            .serve(listener);
    });
    let client = std::sync::Arc::new(Client::connect(addr).unwrap());

    // Hold the one call slot with a request that sleeps.
    let busy = std::sync::Arc::clone(&client);
    let held = std::thread::spawn(move || {
        busy.call(
            SLEEP,
            &SleepRequest { ms: 500 },
            Some(Duration::from_secs(5)),
        )
    });
    std::thread::sleep(Duration::from_millis(100));

    let err = client
        .call(
            SAY,
            &EchoRequest {
                text: "overflow".to_string(),
                shout: false,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap_err();
    assert_eq!(err.code(), Some(Code::RESOURCE_EXHAUSTED));

    // The slow call still completes, and the freed slot serves the next one.
    held.join().unwrap().unwrap();
    let reply = client
        .call(
            SAY,
            &EchoRequest {
                text: "slot freed".to_string(),
                shout: false,
            },
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(reply.text, "slot freed");
}
