//! The shared echo service: method descriptors plus the reference server
//! implementation. Every language's interop suite talks to this service,
//! so its observable behavior is part of the test contract.

include!(concat!(env!("OUT_DIR"), "/echo.rs"));

pub use interop_echo::{
    CountRequest, CountResponse, EchoRequest, EchoResponse, Empty, FailRequest, SleepRequest,
};

use std::time::Duration;

use nanorpc::{Code, Context, Method, Router, Status, StreamMethod, messages};

messages!(
    EchoRequest,
    EchoResponse,
    CountRequest,
    CountResponse,
    FailRequest,
    SleepRequest,
    Empty
);

pub const SAY: Method<EchoRequest, EchoResponse> = Method::new("interop.Echo/say");
pub const COUNT: StreamMethod<CountRequest, CountResponse> =
    StreamMethod::new("interop.Echo/count");
pub const FAIL: Method<FailRequest, Empty> = Method::new("interop.Echo/fail");
pub const SLEEP: Method<SleepRequest, Empty> = Method::new("interop.Echo/sleep");

/// The reference router. Behavior matrix:
/// `say` echoes (optionally uppercased), `count` streams `1..=up_to`,
/// `fail` closes with the requested status, `sleep` waits while honoring
/// cancellation and the deadline.
pub fn router() -> Router {
    Router::new()
        .unary(SAY, |_ctx, req: EchoRequest| {
            let text = if req.shout {
                req.text.to_uppercase()
            } else {
                req.text
            };
            Ok(EchoResponse { text })
        })
        .stream(COUNT, |_ctx, req: CountRequest, sink| {
            for value in 1..=req.up_to {
                sink.send(&CountResponse { value })?;
                if req.fail_after == Some(value) {
                    return Err(Status::new(Code::ABORTED, "failed mid-stream as requested"));
                }
            }
            Ok(())
        })
        .unary(FAIL, |_ctx, req: FailRequest| -> Result<Empty, Status> {
            Err(Status::new(Code(req.code), req.detail))
        })
        .unary(SLEEP, |ctx: &Context, req: SleepRequest| {
            let mut left = u64::from(req.ms);
            while left > 0 {
                if ctx.is_cancelled() {
                    return Err(Status::new(Code::CANCELLED, "woken by cancellation"));
                }
                let nap = left.min(5);
                std::thread::sleep(Duration::from_millis(nap));
                left -= nap;
            }
            Ok(Empty {})
        })
}
