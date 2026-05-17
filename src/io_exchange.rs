//! Single-slot rendezvous channel implementing both [`IoSink`] and [`IoStream`].
//!
//! [`IoExchange`] transfers at most one item at a time between a writer
//! (the [`IoSink`] side) and a reader (the [`IoStream`] side).
//!
//! # State machine
//!
//! The exchange is driven by an atomic `u8` state with the following
//! transitions:
//!
//! ```text
//!   EMPTY ──send──► FULL ──read──► EMPTY  (normal cycle)
//!
//!   EMPTY ──flush──► EMPTY_FLUSH ──reader sees──► EMPTY_FLUSHED
//!   FULL  ──flush──► FULL_FLUSH  ──read──► EMPTY_FLUSH ──reader sees──► EMPTY_FLUSHED
//!
//!   EMPTY* ──close──► DONE
//!   FULL*  ──close──► FULL_CLOSED ──read──► DONE
//!
//!   (any)  ──drop_read──► DROPPED
//! ```
//!
//! "reader sees" means the reader called [`con_poll_read`](IoStream::con_poll_read) and
//! observed the empty slot, which proves it has consumed everything sent so far.
//!
//! # Synchronization
//!
//! - The `state` field (a [`State`] enum) records the current lifecycle
//!   phase.
//! - [`WakerRef`]s for both sides ensure the other side is notified when
//!   it can make progress.

use core::task::{Context, Poll};

use crate::{IoError, io_sink::IoSink, io_stream::IoStream, waker_ref::WakerRef};

/// A single-slot rendezvous channel.
///
/// Implements both [`IoSink`] (writer) and [`IoStream`] (reader).  The
/// trait methods take `&mut self`; callers that share the exchange
/// between separate producer and consumer tasks must arrange exclusive
/// access (e.g. by wrapping it in a lock).
///
/// # Usage
///
/// Embed `IoExchange` fields in the IO struct used by a `ProcMachine`'s
/// internal tasks and external code. Tasks use the [`IoSink`] /
/// [`IoStream`] methods to send and receive items, while external code
/// interacts through the [`IoGuard`](crate::IoGuard).
#[derive(Debug)]
pub struct IoExchange<ITEM> {
    /// Waker for the reader side, notified when an item is placed or the
    /// stream is closed.
    reader: WakerRef,
    /// Waker for the writer side, notified when the reader consumes the
    /// item (freeing the slot) or drops.
    writer: WakerRef,
    /// State machine governing the exchange lifecycle.
    state: State,
    /// The single-item payload slot.
    item: Option<ITEM>,
}

/// Lifecycle phase of an [`IoExchange`].
///
/// `Empty*` variants describe an empty slot (no item in flight),
/// `Full*` variants describe a full slot (item waiting for the reader),
/// and `Done` / `Dropped` are terminal.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum State {
    /// Slot is empty, no flush requested.
    Empty,
    /// Slot is empty, writer has requested a flush but the reader hasn't
    /// acknowledged yet.
    EmptyFlush,
    /// Slot is empty and the reader has acknowledged the flush (observed
    /// the empty slot after the flush request).
    EmptyFlushed,
    /// Slot contains an item, no flush/close pending.
    Full,
    /// Slot contains an item and a flush has been requested.
    FullFlush,
    /// Slot contains the final item; after the reader takes it the
    /// stream ends.
    FullClosed,
    /// Stream is finished — the reader will see `None` from now on.
    Done,
    /// The reader has been dropped; further writes will error.
    Dropped,
}

impl<ITEM> Default for IoExchange<ITEM> {
    fn default() -> Self {
        Self::new()
    }
}

impl<ITEM> IoExchange<ITEM> {
    /// Creates a new, empty exchange in the `EMPTY` state.
    pub fn new() -> Self {
        Self {
            reader: WakerRef::new(),
            writer: WakerRef::new(),
            state: State::Empty,
            item: None,
        }
    }

    /// Resets the exchange to its initial `EMPTY` state, discarding any
    /// in-flight item and waking any registered reader or writer so they
    /// re-poll and observe the fresh state.
    pub fn reset(&mut self) {
        self.state = State::Empty;
        self.item = None;
        self.reader.wake();
        self.writer.wake();
    }
}

// ---------------------------------------------------------------------------
// IoStream (reader side)
// ---------------------------------------------------------------------------

impl<ITEM> IoStream for IoExchange<ITEM> {
    type Item = ITEM;
    type Error = IoError;

    /// Takes the next item from the exchange.
    ///
    /// State transitions on read:
    /// - `FULL` → `EMPTY` (normal cycle)
    /// - `FULL_FLUSH` → `EMPTY_FLUSH` (item consumed, flush still pending)
    /// - `FULL_CLOSED` → `DONE` (last item consumed, stream ends)
    fn con_poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<ITEM>, Self::Error>> {
        let st = self.state;
        let nextst = match st {
            State::EmptyFlushed => {
                self.reader.register(cx.waker());
                return Poll::Pending;
            }
            State::Empty | State::EmptyFlush => {
                // Acknowledge the flush (reader has seen the empty slot).
                self.reader.register(cx.waker());
                self.state = State::EmptyFlushed;
                self.writer.wake();
                return Poll::Pending;
            }
            State::Full => State::Empty,
            State::FullFlush => State::EmptyFlush,
            State::FullClosed => State::Done,
            // Terminal EOS — repeatable; per the con_poll* contract we
            // do not pre-wake.
            State::Done => {
                return Poll::Ready(Ok(None));
            }
            // DROPPED — persistent error; no pre-wake.
            _ => {
                return Poll::Ready(Err(IoError::InvalidState));
            }
        };

        self.state = nextst;
        let item = self.item.take();
        self.writer.wake();

        if let Some(item) = item {
            // Item consumed; pre-wake to drain more.
            cx.waker().wake_by_ref();
            Poll::Ready(Ok(Some(item)))
        } else if nextst == State::Done {
            // Last item slot was empty but state advanced to Done —
            // surface EOS without pre-waking.
            Poll::Ready(Ok(None))
        } else {
            // The state said FULL but the item slot was empty — shouldn't happen
            // in correct usage. Self-wake and pend so the caller retries.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn drop_read(&mut self) {
        let st = self.state;
        match st {
            State::Dropped | State::Done => (),
            _ => {
                // Move to DROPPED and discard any in-flight item.
                self.state = State::Dropped;
                self.item = None;
                self.writer.wake();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IoSink (writer side)
// ---------------------------------------------------------------------------

impl<ITEM> IoSink<ITEM> for IoExchange<ITEM> {
    type Error = IoError;

    /// Checks whether the slot is empty and ready to accept a new item.
    fn prod_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state;
        match st {
            // Any empty state means the slot is free.
            State::Empty | State::EmptyFlush | State::EmptyFlushed => Poll::Ready(Ok(())),
            // Slot occupied — wait for the reader to consume.
            State::Full | State::FullFlush | State::FullClosed => {
                self.writer.register(cx.waker());
                Poll::Pending
            }
            State::Dropped => Poll::Ready(Err(IoError::BrokenPipe)),
            _ => Poll::Ready(Err(IoError::InvalidState)),
        }
    }

    /// Places an item into the slot if it is empty.
    fn prod_poll_send(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), IoError>> {
        if item.is_none() {
            return Poll::Ready(Ok(()));
        }
        let st = self.state;
        match st {
            State::Empty | State::EmptyFlush | State::EmptyFlushed => {
                self.state = State::Full;
                self.item = item.take();
                self.reader.wake();
                Poll::Ready(Ok(()))
            }
            State::Full | State::FullFlush | State::FullClosed => {
                self.writer.register(cx.waker());
                Poll::Pending
            }
            State::Dropped => Poll::Ready(Err(IoError::BrokenPipe)),
            _ => Poll::Ready(Err(IoError::InvalidState)),
        }
    }

    /// Requests that the reader acknowledge all previously sent items.
    ///
    /// Flush is a two-phase handshake:
    /// 1. Writer transitions to a `*_FLUSH` state and wakes the reader.
    /// 2. Reader observes the empty slot (via `con_poll_read`)
    ///    and transitions to `EMPTY_FLUSHED`.
    /// 3. Writer sees `EMPTY_FLUSHED` and returns `Ready`.
    fn prod_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state;
        match st {
            // Already in a flush/close state — wait for reader progress.
            State::EmptyFlush | State::FullFlush | State::FullClosed => {
                self.writer.register(cx.waker());
                Poll::Pending
            }
            // Reader has acknowledged the flush.
            State::EmptyFlushed => Ok(()).into(),
            // Slot is empty — request a flush.
            State::Empty => {
                self.writer.register(cx.waker());
                self.state = State::EmptyFlush;
                self.reader.wake();
                Poll::Pending
            }
            // Item still in flight — mark as flush-pending.
            State::Full => {
                self.writer.register(cx.waker());
                self.state = State::FullFlush;
                self.reader.wake();
                Poll::Pending
            }
            // DONE or DROPPED — nothing left to flush.
            _ => Ok(()).into(),
        }
    }

    /// Signals that no more items will be sent and waits for close completion.
    ///
    /// If the slot is empty, transitions directly to `DONE`. If an item is
    /// still in flight, transitions to `FULL_CLOSED` so the reader gets the
    /// last item before seeing end-of-stream.
    fn prod_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state;
        match st {
            // Empty (any sub-state) — go straight to DONE.
            State::Empty | State::EmptyFlush | State::EmptyFlushed => {
                self.state = State::Done;
                self.reader.wake();
                Ok(()).into()
            }
            // Item in flight — mark as "last item, then close".
            State::Full | State::FullFlush => {
                self.writer.register(cx.waker());
                self.state = State::FullClosed;
                self.reader.wake();
                Poll::Pending
            }
            // Already waiting for the reader to take the last item.
            State::FullClosed => {
                self.writer.register(cx.waker());
                Poll::Pending
            }
            // DONE or DROPPED — already finished.
            _ => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::task::Context;

    type TestExchange = IoExchange<u64>;

    /// Helper: run a closure with a no-op waker context.
    fn with_noop_cx<T>(f: impl FnOnce(&mut Context<'_>) -> T) -> T {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        f(&mut cx)
    }

    #[test]
    fn send_receive_single_item() {
        let mut r = TestExchange::new();

        let pending = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(pending, Poll::Pending));

        let ready = with_noop_cx(|cx| r.prod_poll_ready(cx));
        assert!(matches!(ready, Poll::Ready(Ok(()))));

        match with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(42))) {
            Poll::Ready(Ok(_)) => (),
            _ => panic!(),
        }

        let pending = with_noop_cx(|cx| r.prod_poll_ready(cx));
        assert!(matches!(pending, Poll::Pending));

        let next = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(next, Poll::Ready(Ok(Some(42)))));

        let ready = with_noop_cx(|cx| r.prod_poll_ready(cx));
        assert!(matches!(ready, Poll::Ready(Ok(()))));

        let pending = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(pending, Poll::Pending));
    }

    #[test]
    fn flush_on_empty_requires_check() {
        let mut r = TestExchange::new();

        let flushed = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(flushed, Poll::Pending));

        let flushed = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(flushed, Poll::Pending));

        let read = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(read, Poll::Pending));

        let flushed = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(flushed, Poll::Ready(Ok(()))));

        let ready = with_noop_cx(|cx| r.prod_poll_ready(cx));
        assert!(matches!(ready, Poll::Ready(Ok(()))));
    }

    #[test]
    fn flush_waits_for_in_flight_item_and_check() {
        let mut r = TestExchange::new();
        match with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(7))) {
            Poll::Ready(Ok(_)) => (),
            _ => panic!(),
        }

        let pending = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(pending, Poll::Pending));

        let next = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(next, Poll::Ready(Ok(Some(7)))));

        let flushed = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(flushed, Poll::Pending));

        let next = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(next, Poll::Pending));

        let flushed = with_noop_cx(|cx| r.prod_poll_flush(cx));
        assert!(matches!(flushed, Poll::Ready(Ok(()))));
    }

    #[test]
    fn close_when_empty_finishes_stream() {
        let mut r = TestExchange::new();

        let closed = with_noop_cx(|cx| r.prod_poll_close(cx));
        assert!(matches!(closed, Poll::Ready(Ok(()))));

        let end = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(end, Poll::Ready(Ok(None))));
    }

    #[test]
    fn close_after_full_delivers_last_item() {
        let mut r = TestExchange::new();
        match with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(11))) {
            Poll::Ready(Ok(_)) => (),
            _ => panic!(),
        }

        let pending = with_noop_cx(|cx| r.prod_poll_close(cx));
        assert!(matches!(pending, Poll::Pending));

        let next = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(next, Poll::Ready(Ok(Some(11)))));

        let end = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(end, Poll::Ready(Ok(None))));

        let closed = with_noop_cx(|cx| r.prod_poll_close(cx));
        assert!(matches!(closed, Poll::Ready(Ok(()))));
    }

    #[test]
    fn start_send_on_full_is_invalid_state() {
        let mut r = TestExchange::new();
        match with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(1))) {
            Poll::Ready(Ok(_)) => (),
            _ => panic!(),
        }
        match with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(2))) {
            Poll::Pending => (),
            _ => panic!(),
        }
    }

    #[test]
    fn reader_dropped_errors() {
        let mut r = TestExchange::new();
        r.state = State::Dropped;

        let ready = with_noop_cx(|cx| r.prod_poll_ready(cx));
        assert!(matches!(ready, Poll::Ready(Err(IoError::BrokenPipe))));
        let ready = with_noop_cx(|cx| r.prod_poll_send(cx, &mut Some(1)));
        assert!(matches!(ready, Poll::Ready(Err(IoError::BrokenPipe))));
    }

    // A minimal `Waker` that records how many times it was woken.
    struct CountWaker(std::sync::atomic::AtomicUsize);
    impl CountWaker {
        fn new() -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self(std::sync::atomic::AtomicUsize::new(0)))
        }
        fn count(&self) -> usize {
            self.0.load(AtomicOrdering::SeqCst)
        }
        fn reset(&self) {
            self.0.store(0, AtomicOrdering::SeqCst);
        }
    }
    impl std::task::Wake for CountWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    #[test]
    fn con_poll_read_pre_wakes_on_consume_but_not_eof() {
        // Pre-wake when an item is consumed; do NOT pre-wake when EOS is
        // observed, since the signal is terminal and repeatable.
        let mut r = TestExchange::new();
        let cw = CountWaker::new();
        let w: std::task::Waker = cw.clone().into();
        let mut cx = Context::from_waker(&w);

        match r.prod_poll_send(&mut cx, &mut Some(99)) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected send Ok, got {:?}", other),
        }
        cw.reset();
        match r.con_poll_read(&mut cx) {
            Poll::Ready(Ok(Some(99))) => {}
            other => panic!("expected Ready(Some(99)), got {:?}", other),
        }
        assert!(cw.count() > 0, "consuming an item must pre-wake");

        match r.prod_poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected close Ok, got {:?}", other),
        }
        cw.reset();
        match r.con_poll_read(&mut cx) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected Ready(None), got {:?}", other),
        }
        assert_eq!(cw.count(), 0, "EOS must not pre-wake");
    }
}
