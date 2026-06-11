"""Python client against the Rust reference server.

The harness builds and starts `interop-server` (a real process, real
sockets) and drives it with the Python client. Behavior must match the
Rust loopback suite in `interop/tests/loopback.rs`.
"""

import subprocess
import time

import pytest

import echo
import nanorpc

SAY = nanorpc.Method("interop.Echo/say", echo.EchoRequest, echo.EchoResponse)
COUNT = nanorpc.StreamMethod("interop.Echo/count", echo.CountRequest, echo.CountResponse)
FAIL = nanorpc.Method("interop.Echo/fail", echo.FailRequest, echo.Empty)
SLEEP = nanorpc.Method("interop.Echo/sleep", echo.SleepRequest, echo.Empty)
MISSING = nanorpc.Method("interop.Echo/no_such", echo.EchoRequest, echo.EchoResponse)


@pytest.fixture(scope="module")
def server():
    process = subprocess.Popen(
        ["target/debug/interop-server"],
        stdout=subprocess.PIPE,
        text=True,
    )
    line = process.stdout.readline().strip()
    assert line.startswith("LISTENING "), f"unexpected server output: {line!r}"
    yield int(line.split()[1])
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture()
def client(server):
    client = nanorpc.Client.connect("127.0.0.1", server)
    yield client
    client.close()


def test_unary_round_trip(client):
    reply = client.call(SAY, echo.EchoRequest(text="hello, wire"), deadline=5)
    assert reply.text == "hello, wire"
    loud = client.call(SAY, echo.EchoRequest(text="hello", shout=True), deadline=5)
    assert loud.text == "HELLO"


def test_server_stream_delivers_in_order(client):
    stream = client.server_stream(COUNT, echo.CountRequest(up_to=100), deadline=5)
    assert [item.value for item in stream] == list(range(1, 101))


def test_mid_stream_failure_carries_the_status(client):
    stream = client.server_stream(
        COUNT, echo.CountRequest(up_to=10, fail_after=3), deadline=5
    )
    assert next(stream).value == 1
    assert next(stream).value == 2
    assert next(stream).value == 3
    with pytest.raises(nanorpc.RpcError) as failure:
        next(stream)
    assert failure.value.code == nanorpc.ABORTED
    assert failure.value.message == "failed mid-stream as requested"


def test_error_statuses_survive_the_wire(client):
    with pytest.raises(nanorpc.RpcError) as failure:
        client.call(
            FAIL,
            echo.FailRequest(code=nanorpc.PERMISSION_DENIED, detail="no such key"),
            deadline=5,
        )
    assert failure.value.code == nanorpc.PERMISSION_DENIED
    assert failure.value.message == "no such key"


def test_unknown_methods_are_unimplemented(client):
    with pytest.raises(nanorpc.RpcError) as failure:
        client.call(MISSING, echo.EchoRequest(text=""), deadline=5)
    assert failure.value.code == nanorpc.UNIMPLEMENTED


def test_deadlines_cut_calls_short(client):
    started = time.monotonic()
    with pytest.raises(nanorpc.RpcError) as failure:
        client.call(SLEEP, echo.SleepRequest(ms=5000), deadline=0.08)
    assert failure.value.code == nanorpc.DEADLINE_EXCEEDED
    assert time.monotonic() - started < 2

    reply = client.call(SAY, echo.EchoRequest(text="still alive"), deadline=5)
    assert reply.text == "still alive"


def test_cancelling_a_stream_frees_the_connection(client):
    stream = client.server_stream(
        COUNT, echo.CountRequest(up_to=0xFFFFFFFF), deadline=5
    )
    assert next(stream).value == 1
    stream.cancel()

    reply = client.call(SAY, echo.EchoRequest(text="after cancel"), deadline=5)
    assert reply.text == "after cancel"


def test_large_payloads_round_trip(client):
    text = "x" * (1 << 20)
    reply = client.call(SAY, echo.EchoRequest(text=text), deadline=10)
    assert reply.text == text


def test_concurrent_calls_share_one_connection(client):
    import threading

    failures = []

    def worker(i):
        try:
            reply = client.call(SAY, echo.EchoRequest(text=f"call {i}"), deadline=5)
            assert reply.text == f"call {i}"
        except Exception as err:  # noqa: BLE001 - collected for the main thread
            failures.append(err)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not failures
