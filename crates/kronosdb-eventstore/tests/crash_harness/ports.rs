//! Ports for the servers a test starts as child processes.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};

/// Below the ephemeral ranges: Linux hands out 32768 and up, macOS 49152 and
/// up, to outgoing connections and to binds on port 0.
const FIRST: u32 = 20_000;
const END: u32 = 32_768;

/// A localhost port nothing listens on, for a server this test starts.
///
/// The child binds it some time after this returns. A port the kernel gave
/// out for `127.0.0.1:0` sits in the ephemeral range, where the kernel hands
/// it straight on to the next outgoing connection, and the child then fails
/// with "address in use". Ports below that range are only ever taken by an
/// explicit bind. Each test process walks the range from its own starting
/// point, so two processes running at once start far apart, and a port
/// something already listens on is skipped. No port is given out twice in
/// one process.
pub fn free_port() -> u16 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let span = END - FIRST;
    let start = std::process::id().wrapping_mul(7919) % span;
    loop {
        let taken = NEXT.fetch_add(1, Ordering::Relaxed);
        assert!(taken < span, "every test port in {FIRST}..{END} is in use");
        let port = (FIRST + (start + taken) % span) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}
