//! Byte streams an executor can attach over.
//!
//! The frame protocol only needs `Read + Write`, so a transport's whole job is
//! to hand back owned halves — a reader thread and the dispatch thread use the
//! connection concurrently — plus a read timeout so a silent peer cannot pin
//! the handshake, and a peer label for logs.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Owned read half of a split transport.
pub type ReadHalf = Box<dyn BufRead + Send>;
/// Owned write half of a split transport.
pub type WriteHalf = Box<dyn Write + Send>;

/// Controls a connection whose halves have already been split.
///
/// Both knobs have to outlive the split. The read timeout bounds the handshake
/// so a silent peer cannot pin an attach, then clears so an executor idling
/// between jobs is not dropped. [`Connection::close`] is what makes a blocked
/// reader return, so shutdown cannot hang on a peer that stops responding.
pub struct Connection {
    set_read_timeout: Box<dyn Fn(Option<Duration>) -> io::Result<()> + Send + Sync>,
    set_write_timeout: Box<dyn Fn(Option<Duration>) -> io::Result<()> + Send + Sync>,
    close: Box<dyn Fn() + Send + Sync>,
}

impl Connection {
    /// Bound how long a read may block. `None` blocks indefinitely.
    ///
    /// A timed-out read reports [`io::ErrorKind::WouldBlock`], matching the
    /// platform socket behaviour the socket transports inherit.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        (self.set_read_timeout)(timeout)
    }

    /// Bound how long a write may block. `None` blocks indefinitely.
    ///
    /// A peer that stops reading fills the kernel send buffer, and an unbounded
    /// write would then park the dispatch thread for good.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        (self.set_write_timeout)(timeout)
    }

    /// Tear the connection down so a reader blocked on it returns. Idempotent
    /// and best-effort: the peer may already be gone.
    pub fn close(&self) {
        (self.close)();
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Connection")
    }
}

/// A bidirectional stream carrying the worker frame protocol.
pub trait Transport: Send {
    /// Split into owned halves — so a reader thread and the dispatch thread can
    /// use the connection at the same time — plus its lifetime controls.
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)>;

    /// Peer label for logs. Never carries credentials.
    fn peer(&self) -> String;
}

/// Attach over a Unix domain socket — the same-pod sidecar case.
#[cfg(unix)]
pub struct UnixTransport(UnixStream);

#[cfg(unix)]
impl UnixTransport {
    /// Wrap an accepted or connected stream.
    pub fn new(stream: UnixStream) -> Self {
        Self(stream)
    }
}

#[cfg(unix)]
impl Transport for UnixTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        // Every clone shares one fd, so the control reaches the read half.
        let read = self.0.try_clone()?;
        let control = Arc::new(self.0.try_clone()?);
        let writer_control = control.clone();
        let closer = control.clone();
        Ok((
            Box::new(BufReader::new(read)),
            Box::new(self.0),
            Connection {
                set_read_timeout: Box::new(move |timeout| control.set_read_timeout(timeout)),
                set_write_timeout: Box::new(move |timeout| {
                    writer_control.set_write_timeout(timeout)
                }),
                close: Box::new(move || {
                    let _ = closer.shutdown(std::net::Shutdown::Both);
                }),
            },
        ))
    }

    fn peer(&self) -> String {
        match self.0.peer_addr().ok().and_then(|a| {
            a.as_pathname()
                .map(|p| p.to_string_lossy().into_owned())
                .filter(|p| !p.is_empty())
        }) {
            Some(path) => format!("unix:{path}"),
            None => "unix:unnamed".to_string(),
        }
    }
}

/// Attach over TCP — an executor in another pod or host.
pub struct TcpTransport(TcpStream);

impl TcpTransport {
    /// Wrap an accepted or connected stream. Nagle is disabled: frames are
    /// small and latency-sensitive, and a job dispatch must not wait on
    /// coalescing.
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self(stream))
    }
}

impl Transport for TcpTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        let read = self.0.try_clone()?;
        let control = Arc::new(self.0.try_clone()?);
        let writer_control = control.clone();
        let closer = control.clone();
        Ok((
            Box::new(BufReader::new(read)),
            Box::new(self.0),
            Connection {
                set_read_timeout: Box::new(move |timeout| control.set_read_timeout(timeout)),
                set_write_timeout: Box::new(move |timeout| {
                    writer_control.set_write_timeout(timeout)
                }),
                close: Box::new(move || {
                    let _ = closer.shutdown(std::net::Shutdown::Both);
                }),
            },
        ))
    }

    fn peer(&self) -> String {
        match self.0.peer_addr() {
            Ok(addr) => format!("tcp:{addr}"),
            Err(_) => "tcp:unknown".to_string(),
        }
    }
}

/// In-process transport pair, for tests and for embedding an executor in the
/// scheduler process without a socket.
pub struct MemoryTransport {
    incoming: Arc<Channel>,
    outgoing: Arc<Channel>,
    label: String,
}

impl MemoryTransport {
    /// Build two ends wired to each other. Dropping one end's write half
    /// signals EOF to the other's reader.
    pub fn pair() -> (Self, Self) {
        let left = Arc::new(Channel::default());
        let right = Arc::new(Channel::default());
        (
            Self {
                incoming: left.clone(),
                outgoing: right.clone(),
                label: "memory:a".to_string(),
            },
            Self {
                incoming: right,
                outgoing: left,
                label: "memory:b".to_string(),
            },
        )
    }
}

impl Transport for MemoryTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        self.outgoing.open_writer();
        let control = self.incoming.clone();
        let closer = self.incoming.clone();
        Ok((
            Box::new(BufReader::new(ChannelReader {
                channel: self.incoming,
            })),
            Box::new(ChannelWriter {
                channel: self.outgoing,
            }),
            Connection {
                set_read_timeout: Box::new(move |timeout| {
                    *control.read_timeout.lock().unwrap_or_else(recover) = timeout;
                    Ok(())
                }),
                // The in-memory buffer is unbounded, so a write never blocks.
                set_write_timeout: Box::new(|_| Ok(())),
                close: Box::new(move || closer.close_reader()),
            },
        ))
    }

    fn peer(&self) -> String {
        self.label.clone()
    }
}

/// Recover a guard from a poisoned lock rather than cascading the panic — the
/// state behind it is a plain buffer, so reading it is always safe.
fn recover<T>(poisoned: std::sync::PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// One direction of a [`MemoryTransport`] pair.
#[derive(Default)]
struct Channel {
    state: Mutex<ChannelState>,
    ready: Condvar,
    read_timeout: Mutex<Option<Duration>>,
}

#[derive(Default)]
struct ChannelState {
    buffer: VecDeque<u8>,
    writer_open: bool,
    writer_ever_opened: bool,
}

impl Channel {
    fn open_writer(&self) {
        let mut state = self.state.lock().unwrap_or_else(recover);
        state.writer_open = true;
        state.writer_ever_opened = true;
    }

    fn close_writer(&self) {
        let mut state = self.state.lock().unwrap_or_else(recover);
        state.writer_open = false;
        drop(state);
        self.ready.notify_all();
    }

    /// Force this direction to EOF, the in-memory analogue of a socket
    /// shutdown, so a blocked reader returns.
    fn close_reader(&self) {
        let mut state = self.state.lock().unwrap_or_else(recover);
        state.writer_open = false;
        state.writer_ever_opened = true;
        state.buffer.clear();
        drop(state);
        self.ready.notify_all();
    }
}

struct ChannelReader {
    channel: Arc<Channel>,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let timeout = *self.channel.read_timeout.lock().unwrap_or_else(recover);
        let mut state = self.channel.state.lock().unwrap_or_else(recover);

        while state.buffer.is_empty() {
            // Before the peer splits, `writer_open` is false but no data can
            // have been lost — treating that as EOF would race the handshake.
            if state.writer_ever_opened && !state.writer_open {
                return Ok(0);
            }
            state = match timeout {
                None => self.channel.ready.wait(state).unwrap_or_else(recover),
                Some(limit) => {
                    let (guard, result) = self
                        .channel
                        .ready
                        .wait_timeout(state, limit)
                        .unwrap_or_else(recover);
                    if result.timed_out() && guard.buffer.is_empty() {
                        return Err(io::Error::new(io::ErrorKind::WouldBlock, "read timed out"));
                    }
                    guard
                }
            };
        }

        let count = out.len().min(state.buffer.len());
        for (slot, byte) in out.iter_mut().zip(state.buffer.drain(..count)) {
            *slot = byte;
        }
        Ok(count)
    }
}

struct ChannelWriter {
    channel: Arc<Channel>,
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut state = self.channel.state.lock().unwrap_or_else(recover);
        state.buffer.extend(data);
        drop(state);
        self.channel.ready.notify_all();
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ChannelWriter {
    fn drop(&mut self) {
        self.channel.close_writer();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_pair_carries_bytes_both_ways() {
        let (a, b) = MemoryTransport::pair();
        let (mut a_read, mut a_write, _) = Box::new(a).split().expect("split a");
        let (mut b_read, mut b_write, _) = Box::new(b).split().expect("split b");

        a_write.write_all(b"ping\n").expect("write");
        a_write.flush().expect("flush");
        let mut line = String::new();
        b_read.read_line(&mut line).expect("read");
        assert_eq!(line, "ping\n");

        b_write.write_all(b"pong\n").expect("write");
        b_write.flush().expect("flush");
        line.clear();
        a_read.read_line(&mut line).expect("read");
        assert_eq!(line, "pong\n");
    }

    #[test]
    fn dropping_the_writer_signals_eof() {
        let (a, b) = MemoryTransport::pair();
        let (_a_read, a_write, _) = Box::new(a).split().expect("split a");
        let (mut b_read, _b_write, _) = Box::new(b).split().expect("split b");

        drop(a_write);
        let mut buf = Vec::new();
        assert_eq!(b_read.read_to_end(&mut buf).expect("read"), 0);
    }

    #[test]
    fn read_timeout_applies_and_clears_after_the_split() {
        let (a, b) = MemoryTransport::pair();
        let (_a_read, mut a_write, _) = Box::new(a).split().expect("split a");
        let (mut b_read, _b_write, timeout) = Box::new(b).split().expect("split b");

        timeout
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set timeout");
        let mut byte = [0u8; 1];
        let err = b_read.read(&mut byte).expect_err("must time out");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        // Clearing must reach the already-split read half — otherwise an idle
        // executor would be dropped the moment its handshake budget elapsed.
        timeout.set_read_timeout(None).expect("clear timeout");
        let reader = std::thread::spawn(move || {
            let mut buf = [0u8; 4];
            b_read.read_exact(&mut buf).expect("blocking read");
            buf
        });
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            !reader.is_finished(),
            "a cleared timeout must block, not expire"
        );

        a_write.write_all(b"ping").expect("write");
        a_write.flush().expect("flush");
        assert_eq!(&reader.join().expect("reader thread")[..], b"ping");
    }
}
