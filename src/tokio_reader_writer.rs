use core::cell::RefCell;
use core::cmp::min;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;

use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_util::io::poll_read_buf;

use crate::AsyncBufProvider;
use crate::IoError;
use crate::IoReader;
use crate::IoWriter;

/// A wrapper around an [`AsyncRead`] that implements the [`IoReader`] trait.
///
/// Read buffers are obtained from the supplied [`AsyncBufProvider`].  The
/// wrapper attempts to amortise allocations by reading additional data into
/// the current buffer when there is enough free space, returning chunks via
/// [`Bytes::split_to`] so that consumers see reference-counted slices into
/// the same underlying allocation.
///
/// # State transitions
///
/// - `con_poll_read` proxies through to the underlying [`AsyncRead`] until
///   EOF or an error is observed.
/// - When the underlying reader reports EOF or an error, the wrapper drains
///   any remaining buffered bytes first, then surfaces the EOF / error and
///   drops the inner reader.
/// - Once the inner reader has been dropped (either by reaching EOF / error,
///   or by an explicit [`drop_read`](IoReader::drop_read)), `con_poll_read`
///   is in the EOF condition: it returns `Ready(Ok(None))` and pre-wakes the
///   caller per the `con_poll*` contract.  The signal is repeatable.
/// - [`drop_read`](IoReader::drop_read) drops the inner reader and discards
///   any buffered data and pending error.
///
/// The inner reader is supplied at construction time and cannot be replaced.
pub struct ReaderIoReader<READER, BP>
where
    READER: AsyncRead + Send + Unpin,
    BP: AsyncBufProvider + Send,
{
    inner: RefCell<ReaderIoReaderInner<READER, BP>>,
}

struct ReaderIoReaderInner<READER, BP>
where
    READER: AsyncRead + Send + Unpin,
    BP: AsyncBufProvider,
{
    reader: Option<READER>,
    buf_provider: BP,
    cur_buf: BytesMut,
    buf_capacity: usize,
    got_eof: Option<Option<IoError>>,
}

impl<READER, BP> ReaderIoReader<READER, BP>
where
    READER: AsyncRead + Send + Unpin,
    BP: AsyncBufProvider + Send,
{
    /// Creates a new `ReaderIoReader` wrapping the given reader and using
    /// `buf_provider` as the source of read buffers.
    pub fn new(inner: READER, buf_provider: BP) -> Self {
        Self {
            inner: RefCell::new(ReaderIoReaderInner {
                reader: Some(inner),
                buf_provider,
                cur_buf: BytesMut::new(),
                buf_capacity: 0,
                got_eof: None,
            }),
        }
    }
}

impl<READER, BP> IoReader for ReaderIoReader<READER, BP>
where
    READER: AsyncRead + Send + Unpin,
    BP: AsyncBufProvider + Send,
{
    type Error = IoError;

    fn con_poll_read(
        &self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<Result<Option<Bytes>, IoError>> {
        let mut inner = self.inner.borrow_mut();
        if inner.reader.is_none() {
            // Already in EOF state; one repetition of the EOS signal is
            // consumed and the caller is pre-woken.
            cx.waker().wake_by_ref();
            return Poll::Ready(Ok(None));
        };
        // attempt to read more data, if it's appropriate to do so.
        if inner.got_eof.is_none() {
            let have_len = inner.cur_buf.len();
            let space = inner.cur_buf.capacity() - have_len;
            let do_read = if space > 0 && space > (inner.buf_capacity >> 2) {
                // We have enough space to augment the current buffer, but we only want to do this under 2 conditions:
                // 1. We don't have any data to return; or
                // 2. The block we're going to return will cross the buffer half-way point
                let half_buf = inner.buf_capacity >> 1;
                have_len == 0
                    || (have_len + space > half_buf && have_len + space <= half_buf + max_len)
            } else if have_len == 0 {
                match inner.buf_provider.poll_get_buf() {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(buf) => {
                        if buf.capacity() < 1 {
                            return Poll::Ready(Err(std::io::ErrorKind::OutOfMemory.into()));
                        }
                        inner.cur_buf = buf;
                        inner.buf_capacity = inner.cur_buf.capacity();
                    }
                }
                true
            } else {
                false
            };
            if do_read {
                let im = &mut *inner;
                match poll_read_buf(Pin::new(im.reader.as_mut().unwrap()), cx, &mut im.cur_buf) {
                    Poll::Ready(Ok(0)) => {
                        im.got_eof = Some(None);
                    }
                    Poll::Ready(Err(e)) => {
                        im.got_eof = Some(Some(e.into()));
                    }
                    _ => {}
                }
            }
        }
        let have_len = inner.cur_buf.len();
        if have_len == 0 {
            if inner.got_eof.is_none() {
                // No data, but not EOF yet; we must be pending on the reader or the buf provider.  Don't pre-wake, just return Pending.
                return Poll::Pending;
            }
            // EOF condition
            cx.waker().wake_by_ref();
            inner.reader = None;
            let err = inner.got_eof.take().unwrap_or(None);
            return match err {
                None => Poll::Ready(Ok(None)),
                Some(e) => Poll::Ready(Err(e)),
            };
        }
        // We have data
        if max_len == 0 {
            // Caller was just checking.  Not a consumption, so don't pre-wake, but return the empty probe result.
            return Poll::Ready(Ok(Some(Bytes::new())));
        }
        // no matter what now, we're going to consume data or eof or error, so pre-wake the caller.
        cx.waker().wake_by_ref();
        Poll::Ready(Ok(Some(
            inner.cur_buf.split_to(min(max_len, have_len)).freeze(),
        )))
    }

    fn drop_read(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.reader = None;
        inner.got_eof = None;
    }
}

/// A wrapper around an [`AsyncWrite`] that implements the [`IoWriter`] trait.
///
/// All methods proxy to the underlying [`AsyncWrite`]. The inner writer is
/// supplied at construction time and cannot be replaced.
pub struct WriterIoWriter<WRITER>
where
    WRITER: AsyncWrite + Send + Unpin,
{
    inner: RefCell<WRITER>,
}

impl<WRITER> WriterIoWriter<WRITER>
where
    WRITER: AsyncWrite + Send + Unpin,
{
    /// Creates a new `WriterIoWriter` wrapping the given writer.
    pub fn new(inner: WRITER) -> Self {
        Self {
            inner: RefCell::new(inner),
        }
    }
}

impl<WRITER> IoWriter for WriterIoWriter<WRITER>
where
    WRITER: AsyncWrite + Send + Unpin,
{
    type Error = IoError;

    fn prod_poll_write(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<Result<usize, Self::Error>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut inner = self.inner.borrow_mut();
        match Pin::new(&mut *inner).poll_write(cx, bytes.as_ref()) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(sz)) => {
                let _ = bytes.split_to(sz);
                Poll::Ready(Ok(sz))
            }
        }
    }

    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut inner = self.inner.borrow_mut();
        Pin::new(&mut *inner).poll_flush(cx).map_err(Into::into)
    }

    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut inner = self.inner.borrow_mut();
        Pin::new(&mut *inner).poll_shutdown(cx).map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use tokio::io::ReadBuf;

    // -----------------------------------------------------------------------
    // Test waker infrastructure
    // -----------------------------------------------------------------------

    /// A minimal `Waker` that records how many times it was woken.
    struct CountWaker(AtomicUsize);

    impl CountWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self(AtomicUsize::new(0)))
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }

        fn reset(&self) {
            self.0.store(0, Ordering::SeqCst);
        }
    }

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn make_waker() -> (Arc<CountWaker>, std::task::Waker) {
        let cw = CountWaker::new();
        let w: std::task::Waker = cw.clone().into();
        (cw, w)
    }

    // -----------------------------------------------------------------------
    // Mock AsyncRead — driven by a queue of scripted actions.
    // -----------------------------------------------------------------------

    enum ReadAction {
        Pending,
        Data(Vec<u8>),
        Eof,
        Err(io::ErrorKind),
    }

    struct MockReader {
        actions: VecDeque<ReadAction>,
    }

    impl MockReader {
        fn new(actions: Vec<ReadAction>) -> Self {
            Self {
                actions: actions.into(),
            }
        }
    }

    impl tokio::io::AsyncRead for MockReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.actions.pop_front() {
                None => Poll::Ready(Ok(())), // default to EOF
                Some(ReadAction::Eof) => Poll::Ready(Ok(())),
                Some(ReadAction::Pending) => Poll::Pending,
                Some(ReadAction::Data(data)) => {
                    let n = data.len().min(buf.remaining());
                    buf.put_slice(&data[..n]);
                    if n < data.len() {
                        // Push the leftover back at the front for the next poll.
                        self.actions
                            .push_front(ReadAction::Data(data[n..].to_vec()));
                    }
                    Poll::Ready(Ok(()))
                }
                Some(ReadAction::Err(kind)) => Poll::Ready(Err(kind.into())),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Mock BufProvider — script of actions that returns Pending or a
    // particular BytesMut.  After the script is exhausted, returns a fresh
    // 64-byte BytesMut for each call.
    // -----------------------------------------------------------------------

    enum BufAction {
        Pending,
        Buf(BytesMut),
    }

    struct MockBufProvider {
        actions: VecDeque<BufAction>,
        default_cap: usize,
    }

    impl MockBufProvider {
        fn fixed(cap: usize) -> Self {
            Self {
                actions: VecDeque::new(),
                default_cap: cap,
            }
        }

        fn scripted(actions: Vec<BufAction>, default_cap: usize) -> Self {
            Self {
                actions: actions.into(),
                default_cap,
            }
        }
    }

    impl AsyncBufProvider for MockBufProvider {
        fn poll_get_buf(&mut self) -> Poll<BytesMut> {
            match self.actions.pop_front() {
                Some(BufAction::Pending) => Poll::Pending,
                Some(BufAction::Buf(b)) => Poll::Ready(b),
                None => Poll::Ready(BytesMut::with_capacity(self.default_cap)),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Mock AsyncWrite — driven by scripted action queues for each operation.
    // Records every accepted byte in `written`.
    // -----------------------------------------------------------------------

    enum WriteAction {
        Pending,
        Wrote(usize),
        Err(io::ErrorKind),
    }

    enum FlushAction {
        Pending,
        Ok,
        Err(io::ErrorKind),
    }

    enum ShutdownAction {
        Pending,
        Ok,
        Err(io::ErrorKind),
    }

    struct MockWriter {
        write: VecDeque<WriteAction>,
        flush: VecDeque<FlushAction>,
        shutdown: VecDeque<ShutdownAction>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                write: VecDeque::new(),
                flush: VecDeque::new(),
                shutdown: VecDeque::new(),
                written: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_writes(mut self, w: Vec<WriteAction>) -> Self {
            self.write = w.into();
            self
        }

        fn with_flushes(mut self, f: Vec<FlushAction>) -> Self {
            self.flush = f.into();
            self
        }

        fn with_shutdowns(mut self, s: Vec<ShutdownAction>) -> Self {
            self.shutdown = s.into();
            self
        }

        fn written_handle(&self) -> Arc<Mutex<Vec<u8>>> {
            self.written.clone()
        }
    }

    impl tokio::io::AsyncWrite for MockWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.write.pop_front() {
                None => {
                    // Default: accept everything.
                    self.written.lock().unwrap().extend_from_slice(buf);
                    Poll::Ready(Ok(buf.len()))
                }
                Some(WriteAction::Pending) => Poll::Pending,
                Some(WriteAction::Wrote(n)) => {
                    let n = n.min(buf.len());
                    self.written.lock().unwrap().extend_from_slice(&buf[..n]);
                    Poll::Ready(Ok(n))
                }
                Some(WriteAction::Err(kind)) => Poll::Ready(Err(kind.into())),
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.flush.pop_front() {
                None | Some(FlushAction::Ok) => Poll::Ready(Ok(())),
                Some(FlushAction::Pending) => Poll::Pending,
                Some(FlushAction::Err(kind)) => Poll::Ready(Err(kind.into())),
            }
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.shutdown.pop_front() {
                None | Some(ShutdownAction::Ok) => Poll::Ready(Ok(())),
                Some(ShutdownAction::Pending) => Poll::Pending,
                Some(ShutdownAction::Err(kind)) => Poll::Ready(Err(kind.into())),
            }
        }
    }

    // =======================================================================
    // ReaderIoReader tests
    // =======================================================================

    // -----------------------------------------------------------------------
    // After drop_read, the reader is at EOF and the signal is repeatable.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_dropped_returns_eof_and_pre_wakes() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![]);
        let r = ReaderIoReader::new(inner, bp);
        r.drop_read();
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected Ready(Ok(None)), got {:?}", other.map(|_| ())),
        }
        assert!(cw.count() > 0, "EOF must pre-wake the caller");

        // EOF is repeatable.
        cw.reset();
        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected repeated EOF, got {:?}", other.map(|_| ())),
        }
        assert!(cw.count() > 0, "repeated EOF must also pre-wake");
    }

    // -----------------------------------------------------------------------
    // Basic data delivery.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_returns_data_then_eof() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![ReadAction::Data(b"hello".to_vec()), ReadAction::Eof]);
        let r = ReaderIoReader::new(inner, bp);
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"hello"),
            other => panic!("expected Some(hello), got {:?}", other.map(|_| ())),
        }
        assert!(cw.count() > 0, "consuming Ready must pre-wake");

        cw.reset();
        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF, got {:?}", other.map(|_| ())),
        }
        assert!(cw.count() > 0, "EOF must pre-wake");
    }

    // -----------------------------------------------------------------------
    // `max_len` capping splits the buffered data.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_caps_at_max_len() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![
            ReadAction::Data(b"abcdefghij".to_vec()),
            ReadAction::Eof,
        ]);
        let r = ReaderIoReader::new(inner, bp);
        let (_cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        // First call: read into the buffer, cap at 3 bytes.
        match r.con_poll_read(&mut cx, 3) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"abc"),
            other => panic!("expected 'abc', got {:?}", other.map(|_| ())),
        }

        // Subsequent calls drain the rest — the underlying reader is not
        // re-polled (got_eof not yet set, but `do_read` is false because we
        // still have data and not enough free space to bother).
        let mut got = Vec::new();
        loop {
            match r.con_poll_read(&mut cx, 1024) {
                Poll::Ready(Ok(Some(data))) => got.extend_from_slice(&data),
                Poll::Ready(Ok(None)) => break,
                other => panic!("unexpected {:?}", other.map(|_| ())),
            }
        }
        assert_eq!(got, b"defghij");
    }

    // -----------------------------------------------------------------------
    // Underlying reader returning Pending propagates as Pending.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_propagates_pending() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![ReadAction::Pending]);
        let r = ReaderIoReader::new(inner, bp);
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other.map(|_| ())),
        }
        // No consumption, no pre-wake.
        assert_eq!(cw.count(), 0, "Pending must not pre-wake");
    }

    // -----------------------------------------------------------------------
    // Errors are surfaced after any buffered data is drained.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_drains_buffer_before_error() {
        let bp = MockBufProvider::fixed(64);
        // Read 5 bytes, then error.  Both happen on the same poll_read_buf
        // invocation chain because we keep looping inside con_poll_read until
        // there's nothing left to do.
        let inner = MockReader::new(vec![
            ReadAction::Data(b"hello".to_vec()),
            ReadAction::Err(io::ErrorKind::ConnectionReset),
        ]);
        let r = ReaderIoReader::new(inner, bp);
        let (_cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        // First read returns the buffered data.
        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"hello"),
            other => panic!("expected 'hello', got {:?}", other.map(|_| ())),
        }

        // Second read drains nothing more from the buffer, observes the
        // error from the second action, and surfaces it.
        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Err(e)) => assert_eq!(e, IoError::BrokenPipe),
            other => panic!("expected error, got {:?}", other.map(|_| ())),
        }

        // After surfacing the error, the inner reader is set to None — so
        // subsequent reads return plain EOF.
        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF after error, got {:?}", other.map(|_| ())),
        }
    }

    // -----------------------------------------------------------------------
    // `max_len == 0` probes — must not pre-wake when nothing is consumed.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_zero_probe_with_data_is_some_empty_no_prewake() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![ReadAction::Data(b"xy".to_vec()), ReadAction::Eof]);
        let r = ReaderIoReader::new(inner, bp);
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        // Pull some data into the internal buffer.
        let _ = r.con_poll_read(&mut cx, 1024);
        cw.reset();

        // Wait — we just consumed everything, the buffer is empty.  Re-stage
        // by writing data that leaves leftover.  Easier to start over with
        // a different scenario.
        drop(r);

        let bp2 = MockBufProvider::fixed(64);
        let inner2 = MockReader::new(vec![
            ReadAction::Data(b"abcdef".to_vec()),
            ReadAction::Pending,
        ]);
        let r2 = ReaderIoReader::new(inner2, bp2);

        // First read: cap at 3 so 3 bytes remain in the buffer.
        match r2.con_poll_read(&mut cx, 3) {
            Poll::Ready(Ok(Some(d))) => assert_eq!(&d[..], b"abc"),
            other => panic!("expected 'abc', got {:?}", other.map(|_| ())),
        }
        cw.reset();

        // Probe with max_len == 0 — there's data, so we get Some(empty)
        // and the caller is NOT pre-woken.
        match r2.con_poll_read(&mut cx, 0) {
            Poll::Ready(Ok(Some(d))) => assert!(d.is_empty()),
            other => panic!("expected Some(empty), got {:?}", other.map(|_| ())),
        }
        assert_eq!(
            cw.count(),
            0,
            "max_len==0 probe with data must not pre-wake"
        );
    }

    #[test]
    fn reader_zero_probe_eof_is_none_and_pre_wakes() {
        // EOF probe with max_len == 0: per the trait contract, the EOS
        // signal still counts as consumption and pre-wakes the caller.
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![]);
        let r = ReaderIoReader::new(inner, bp);
        r.drop_read();
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 0) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF, got {:?}", other.map(|_| ())),
        }
        assert!(cw.count() > 0, "EOF must pre-wake even on max_len==0 probe");
    }

    // -----------------------------------------------------------------------
    // `drop_read` enters EOF state immediately.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_drop_read_enters_eof() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![ReadAction::Data(b"unread".to_vec())]);
        let r = ReaderIoReader::new(inner, bp);
        let (_cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        r.drop_read();

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF after drop_read, got {:?}", other.map(|_| ())),
        }
    }

    // -----------------------------------------------------------------------
    // BufProvider returning Pending propagates.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_buf_provider_pending_propagates() {
        let bp = MockBufProvider::scripted(vec![BufAction::Pending], 64);
        let inner = MockReader::new(vec![ReadAction::Data(b"x".to_vec())]);
        let r = ReaderIoReader::new(inner, bp);
        let (cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other.map(|_| ())),
        }
        assert_eq!(cw.count(), 0, "buf-provider Pending must not pre-wake");
    }

    // -----------------------------------------------------------------------
    // BufProvider returning a zero-capacity buffer surfaces OutOfMemory.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_zero_capacity_buf_returns_oom() {
        let bp = MockBufProvider::scripted(vec![BufAction::Buf(BytesMut::with_capacity(0))], 0);
        let inner = MockReader::new(vec![ReadAction::Data(b"x".to_vec())]);
        let r = ReaderIoReader::new(inner, bp);
        let (_cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        match r.con_poll_read(&mut cx, 1024) {
            Poll::Ready(Err(e)) => assert_eq!(e, IoError::OutOfMemory),
            other => panic!("expected OutOfMemory, got {:?}", other.map(|_| ())),
        }
    }

    // -----------------------------------------------------------------------
    // Multiple chunks share the same underlying buffer when they fit.
    // -----------------------------------------------------------------------

    #[test]
    fn reader_handles_multiple_chunks() {
        let bp = MockBufProvider::fixed(64);
        let inner = MockReader::new(vec![
            ReadAction::Data(b"aa".to_vec()),
            ReadAction::Data(b"bb".to_vec()),
            ReadAction::Data(b"cc".to_vec()),
            ReadAction::Eof,
        ]);
        let r = ReaderIoReader::new(inner, bp);
        let (_cw, w) = make_waker();
        let mut cx = Context::from_waker(&w);

        let mut all = Vec::new();
        loop {
            match r.con_poll_read(&mut cx, 1024) {
                Poll::Ready(Ok(Some(d))) => all.extend_from_slice(&d),
                Poll::Ready(Ok(None)) => break,
                other => panic!("unexpected {:?}", other.map(|_| ())),
            }
        }
        assert_eq!(all, b"aabbcc");
    }

    // =======================================================================
    // WriterIoWriter tests
    // =======================================================================

    // -----------------------------------------------------------------------
    // Writing empty bytes is a no-op.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_empty_write_is_noop() {
        let w = WriterIoWriter::new(MockWriter::new());
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        let mut empty = Bytes::new();
        match w.prod_poll_write(&mut cx, &mut empty) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("expected Ready(Ok(0)), got {:?}", other.map(|_| ())),
        }
    }

    // -----------------------------------------------------------------------
    // Successful full write — bytes are advanced.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_writes_full_buffer() {
        let mw = MockWriter::new();
        let written = mw.written_handle();
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        let mut payload = Bytes::from_static(b"hello");
        match w.prod_poll_write(&mut cx, &mut payload) {
            Poll::Ready(Ok(5)) => {}
            other => panic!("expected Ok(5), got {:?}", other.map(|_| ())),
        }
        assert!(payload.is_empty(), "payload should be fully consumed");
        assert_eq!(&written.lock().unwrap()[..], b"hello");
    }

    // -----------------------------------------------------------------------
    // Partial write — Bytes is advanced by exactly the number written.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_partial_write_advances_payload() {
        let mw = MockWriter::new().with_writes(vec![WriteAction::Wrote(2)]);
        let written = mw.written_handle();
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        let mut payload = Bytes::from_static(b"abcdef");
        match w.prod_poll_write(&mut cx, &mut payload) {
            Poll::Ready(Ok(2)) => {}
            other => panic!("expected Ok(2), got {:?}", other.map(|_| ())),
        }
        assert_eq!(&payload[..], b"cdef");
        assert_eq!(&written.lock().unwrap()[..], b"ab");
    }

    // -----------------------------------------------------------------------
    // Underlying poll_write Pending propagates and leaves payload intact.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_pending_leaves_payload_untouched() {
        let mw = MockWriter::new().with_writes(vec![WriteAction::Pending]);
        let w = WriterIoWriter::new(mw);
        let (cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        let mut payload = Bytes::from_static(b"xyz");
        match w.prod_poll_write(&mut cx, &mut payload) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other.map(|_| ())),
        }
        assert_eq!(&payload[..], b"xyz");
        assert_eq!(cw.count(), 0, "Pending must not wake by itself");
    }

    // -----------------------------------------------------------------------
    // Underlying error is propagated.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_propagates_error() {
        let mw = MockWriter::new().with_writes(vec![WriteAction::Err(io::ErrorKind::Other)]);
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        let mut payload = Bytes::from_static(b"q");
        match w.prod_poll_write(&mut cx, &mut payload) {
            Poll::Ready(Err(e)) => assert_eq!(e, IoError::Unknown),
            other => panic!("expected Other error, got {:?}", other.map(|_| ())),
        }
        // Per the trait, on error the payload is not advanced.
        assert_eq!(&payload[..], b"q");
    }

    // -----------------------------------------------------------------------
    // Flush proxies to inner.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_flush_proxies() {
        let mw = MockWriter::new().with_flushes(vec![
            FlushAction::Pending,
            FlushAction::Ok,
            FlushAction::Err(io::ErrorKind::WriteZero),
        ]);
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(w.prod_poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(w.prod_poll_flush(&mut cx), Poll::Ready(Ok(()))));
        match w.prod_poll_flush(&mut cx) {
            Poll::Ready(Err(IoError::BrokenPipe)) => {}
            other => panic!("expected BrokenPipe, got {:?}", other.map(|_| ())),
        }
    }

    // -----------------------------------------------------------------------
    // Close pending leaves the inner writer in place — subsequent writes
    // still proxy to it.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_close_pending_keeps_inner() {
        let mw = MockWriter::new().with_shutdowns(vec![ShutdownAction::Pending]);
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(w.prod_poll_close(&mut cx), Poll::Pending));
        let mut payload = Bytes::from_static(b"x");
        match w.prod_poll_write(&mut cx, &mut payload) {
            Poll::Ready(Ok(1)) => {}
            other => panic!(
                "expected write to still succeed, got {:?}",
                other.map(|_| ())
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Successful close proxies through.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_close_ok() {
        let mw = MockWriter::new().with_shutdowns(vec![ShutdownAction::Ok]);
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(w.prod_poll_close(&mut cx), Poll::Ready(Ok(()))));
    }

    // -----------------------------------------------------------------------
    // Close error proxies through.
    // -----------------------------------------------------------------------

    #[test]
    fn writer_close_error_propagates() {
        let mw = MockWriter::new()
            .with_shutdowns(vec![ShutdownAction::Err(io::ErrorKind::ConnectionAborted)]);
        let w = WriterIoWriter::new(mw);
        let (_cw, waker) = make_waker();
        let mut cx = Context::from_waker(&waker);

        match w.prod_poll_close(&mut cx) {
            Poll::Ready(Err(e)) => assert_eq!(e, IoError::AbortRequested),
            other => panic!("expected ConnectionAborted, got {:?}", other.map(|_| ())),
        }
    }
}
