//! Serves the echo service for out-of-process interop suites. Binds an
//! ephemeral port (or `--port N`) and prints `LISTENING <port>` once
//! ready, which is the line test harnesses wait for.

use std::net::TcpListener;

use nanorpc::Server;

fn main() -> std::io::Result<()> {
    let mut port = 0u16;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => {
                let value = args.next().expect("--port takes a number");
                port = value.parse().expect("--port takes a number");
            }
            other => panic!("unknown argument `{other}`"),
        }
    }

    let listener = TcpListener::bind(("127.0.0.1", port))?;
    println!("LISTENING {}", listener.local_addr()?.port());
    Server::new(nanorpc_interop::router()).serve(listener)
}
