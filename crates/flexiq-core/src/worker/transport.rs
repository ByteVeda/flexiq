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
    /// Assemble the controls for a connection this crate did not open.
    ///
    /// Public because [`Transport`] is otherwise unimplementable outside this
    /// crate: `split` has to return one of these, and a transport that carries
    /// something other than a socket — a gRPC stream, a channel — has no
    /// business living in the core just to reach the private fields.
    pub fn new(
        set_read_timeout: impl Fn(Option<Duration>) -> io::Result<()> + Send + Sync + 'static,
        set_write_timeout: impl Fn(Option<Duration>) -> io::Result<()> + Send + Sync + 'static,
        close: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            set_read_timeout: Box::new(set_read_timeout),
            set_write_timeout: Box::new(set_write_timeout),
            close: Box::new(close),
        }
    }

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

    /// Whether this transport established the peer's authority before a single
    /// frame was read, making the `hello` frame's credential redundant.
    ///
    /// False for every byte stream, because a socket carries no credential of
    /// its own — the token in the handshake is the only thing that does. A
    /// transport that answers `true` is asserting that something *outside* the
    /// frame protocol already checked the peer, which is what a gRPC door does
    /// in its auth layer before the RPC is entered. The alternative there would
    /// be filling the configured secret into the executor's own frame on its
    /// behalf, which is forging a credential rather than checking one.
    fn is_authenticated(&self) -> bool {
        false
    }
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
        let left = Arc::new(Channel::unbounded());
        let right = Arc::new(Channel::unbounded());
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
        let reader = self.incoming.reader();
        let writer = self.outgoing.writer();
        let control = self.incoming.clone();
        let closer = self.incoming;
        Ok((
            Box::new(BufReader::new(reader)),
            Box::new(writer),
            Connection::new(
                move |timeout| {
                    control.set_read_timeout(timeout);
                    Ok(())
                },
                // The in-memory buffer is unbounded, so a write never blocks.
                |_| Ok(()),
                move || closer.close_reader(),
            ),
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

/// One direction of an in-process byte stream.
///
/// Backs [`MemoryTransport`] unbounded, and
/// [`FrameTransport`](super::frame_transport::FrameTransport) bounded — a
/// transport whose peer stops reading must fail a write rather than grow a
/// buffer without limit, which is the socket behaviour `set_write_timeout`
/// exists to reproduce.
pub(crate) struct Channel {
    state: Mutex<ChannelState>,
    ready: Condvar,
    read_timeout: Mutex<Option<Duration>>,
    write_timeout: Mutex<Option<Duration>>,
    /// Bytes the buffer may hold before a write blocks. `None` never blocks.
    capacity: Option<usize>,
}

struct ChannelState {
    buffer: VecDeque<u8>,
    writer_open: bool,
    writer_ever_opened: bool,
    reader_open: bool,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            buffer: VecDeque::new(),
            writer_open: false,
            writer_ever_opened: false,
            reader_open: true,
        }
    }
}

impl Channel {
    /// A direction that never blocks a writer.
    pub(crate) fn unbounded() -> Self {
        Self::with_capacity(None)
    }

    /// A direction that blocks a writer once `capacity` bytes are unread.
    pub(crate) fn bounded(capacity: usize) -> Self {
        Self::with_capacity(Some(capacity.max(1)))
    }

    fn with_capacity(capacity: Option<usize>) -> Self {
        Self {
            state: Mutex::new(ChannelState::default()),
            ready: Condvar::new(),
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            capacity,
        }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) {
        *self.read_timeout.lock().unwrap_or_else(recover) = timeout;
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) {
        *self.write_timeout.lock().unwrap_or_else(recover) = timeout;
    }

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
    /// shutdown, so a blocked reader returns — and so a writer blocked on a
    /// full buffer returns too, rather than waiting for a reader that is gone.
    pub(crate) fn close_reader(&self) {
        let mut state = self.state.lock().unwrap_or_else(recover);
        state.writer_open = false;
        state.writer_ever_opened = true;
        state.reader_open = false;
        state.buffer.clear();
        drop(state);
        self.ready.notify_all();
    }

    pub(crate) fn reader(self: &Arc<Self>) -> ChannelReader {
        ChannelReader {
            channel: self.clone(),
        }
    }

    pub(crate) fn writer(self: &Arc<Self>) -> ChannelWriter {
        self.open_writer();
        ChannelWriter {
            channel: self.clone(),
        }
    }
}

pub(crate) struct ChannelReader {
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
        drop(state);
        // Room freed: a bounded writer parked on a full buffer has to be woken
        // here or it waits out its whole timeout for space that already exists.
        self.channel.ready.notify_all();
        Ok(count)
    }
}

/// A reader that goes away is a peer that stopped reading, and a writer blocked
/// on a full buffer must learn that rather than wait for it.
impl Drop for ChannelReader {
    fn drop(&mut self) {
        let mut state = self.channel.state.lock().unwrap_or_else(recover);
        state.reader_open = false;
        drop(state);
        self.channel.ready.notify_all();
    }
}

pub(crate) struct ChannelWriter {
    channel: Arc<Channel>,
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let Some(capacity) = self.channel.capacity else {
            let mut state = self.channel.state.lock().unwrap_or_else(recover);
            state.buffer.extend(data);
            drop(state);
            self.channel.ready.notify_all();
            return Ok(data.len());
        };

        let timeout = *self.channel.write_timeout.lock().unwrap_or_else(recover);
        let mut state = self.channel.state.lock().unwrap_or_else(recover);
        // Partial writes are legal, so a bounded write only blocks when there
        // is no room at all — `write_all` takes care of the rest.
        while state.buffer.len() >= capacity {
            if !state.reader_open {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "the peer stopped reading",
                ));
            }
            state = match timeout {
                None => self.channel.ready.wait(state).unwrap_or_else(recover),
                Some(limit) => {
                    let (guard, result) = self
                        .channel
                        .ready
                        .wait_timeout(state, limit)
                        .unwrap_or_else(recover);
                    if result.timed_out() && guard.buffer.len() >= capacity {
                        return Err(io::Error::new(io::ErrorKind::WouldBlock, "write timed out"));
                    }
                    guard
                }
            };
        }

        let count = data.len().min(capacity - state.buffer.len());
        state.buffer.extend(&data[..count]);
        drop(state);
        self.channel.ready.notify_all();
        Ok(count)
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
