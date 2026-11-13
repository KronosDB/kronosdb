//! What every test that starts a server as a child process shares.

#![allow(dead_code)] // Not every test file uses every helper.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

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

/// How long a server just spawned gets to accept connections, claim its
/// leadership and take writes. A healthy one needs well under a second;
/// this bounds a slow machine under load, not the normal case.
pub const STARTUP: Duration = Duration::from_secs(60);

/// The deadline for a startup wait that begins now.
pub fn startup_deadline() -> Instant {
    Instant::now() + STARTUP
}

/// Starts `command`'s child with nothing open but stdin, stdout and stderr.
///
/// macOS creates a socket and only then marks it close-on-exec, so a child
/// spawned from another thread in between inherits it. A server that
/// inherited the listener [`free_port`] probes with kept that port bound,
/// and the server meant for the port failed with "address in use"; one that
/// inherited a client's connection keeps it open after the client closes it.
/// Linux creates both close-on-exec at once, so there is nothing to undo.
pub fn inherit_only_stdio(command: &mut std::process::Command) {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::process::CommandExt;

        unsafe extern "C" {
            fn getdtablesize() -> i32;
            fn fcntl(fd: i32, cmd: i32, ...) -> i32;
        }
        const F_SETFD: i32 = 2;
        const FD_CLOEXEC: i32 = 1;
        // SAFETY: the hook runs in the child between fork and exec, where
        // only async-signal-safe calls are allowed; these two are.
        unsafe {
            command.pre_exec(|| {
                for fd in 3..getdtablesize() {
                    fcntl(fd, F_SETFD, FD_CLOEXEC);
                }
                Ok(())
            });
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = command;
}
