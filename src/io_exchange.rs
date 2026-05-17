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
//! - The `state` field (`AtomicU8`) is the sequencing authority; all
//!   transitions use `SeqCst` compare-exchanges.
//! - The `item` field (`Mutex<Option<ITEM>>`) protects the payload.
//!   The mutex is held during the brief window of placing/taking the item
//!   so that the state transition and the payload swap are atomic together.
//! - `AtomicWaker`s for both sides ensure the other side is notified when
//!   it can make progress.

use core::{
    sync::atomic::{AtomicU8, Ordering},
    task::{Context, Poll},
};
use futures::task::AtomicWaker;
use parking_lot::Mutex;

use crate::{IoError, io_sink::IoSink, io_stream::IoStream};

/// A single-slot, lock-assisted rendezvous channel.
///
/// Implements both [`IoSink`] (writer) and [`IoStream`] (reader) using
/// interior mutability, so both halves can be accessed through a shared
/// `&IoExchange` reference.
///
/// # Usage
///
/// Embed `IoExchange` fields in the IO struct shared between a
/// `ProcMachine`'s internal tasks and external code. Tasks use the
/// [`IoSink`] / [`IoStream`] methods to send and receive items, while
/// external code interacts through the [`IoGuard`](crate::IoGuard).
#[derive(Debug)]
pub struct IoExchange<ITEM> {
    /// Waker for the reader side, notified when an item is placed or the
    /// stream is closed.
    reader: AtomicWaker,
    /// Waker for the writer side, notified when the reader consumes the
    /// item (freeing the slot) or drops.
    writer: AtomicWaker,
    /// Atomic state machine governing the exchange lifecycle.
    state: AtomicU8,
    /// The single-item payload slot, protected by a mutex so that state
    /// transitions and payload swaps happen together.
    item: Mutex<Option<ITEM>>,
}

// ---------------------------------------------------------------------------
// State constants
// ---------------------------------------------------------------------------

/// Slot is empty, no flush requested.
const EXCH_EMPTY: u8 = 0;
/// Slot is empty, writer has requested a flush but the reader hasn't
/// acknowledged yet.
const EXCH_EMPTY_FLUSH: u8 = 1;
/// Slot is empty and the reader has acknowledged the flush (observed the
/// empty slot after the flush request).
const EXCH_EMPTY_FLUSHED: u8 = 2;
/// Slot contains an item, no flush/close pending.
const EXCH_FULL: u8 = 3;
/// Slot contains an item and a flush has been requested.
const EXCH_FULL_FLUSH: u8 = 4;
/// Slot contains the final item; after the reader takes it the stream ends.
const EXCH_FULL_CLOSED: u8 = 5;
/// Stream is finished — the reader will see `None` from now on.
const EXCH_DONE: u8 = 6;
/// The reader has been dropped; further writes will error.
const EXCH_DROPPED: u8 = 7;

impl<ITEM> Default for IoExchange<ITEM> {
    fn default() -> Self {
        Self::new()
    }
}

impl<ITEM> IoExchange<ITEM> {
    /// Creates a new, empty exchange in the `EMPTY` state.
    pub fn new() -> Self {
        Self {
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            state: AtomicU8::new(EXCH_EMPTY),
            item: Mutex::new(None),
        }
    }

    /// Resets the exchange to its initial `EMPTY` state, discarding any
    /// in-flight item and waking any registered reader or writer so they
    /// re-poll and observe the fresh state.
    pub fn reset(&self) {
        let mut guard = self.item.lock();
        self.state.store(EXCH_EMPTY, Ordering::SeqCst);
        *guard = None;
        drop(guard);
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
    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Result<Option<ITEM>, Self::Error>> {
        let mut guard = self.item.lock();
        let st = self.state.load(Ordering::Acquire);
        let nextst = match st {
            EXCH_EMPTY_FLUSHED => {
                self.reader.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                return Poll::Pending;
            }
            EXCH_EMPTY | EXCH_EMPTY_FLUSH => {
                // Acknowledge the flush (reader has seen the empty slot).
                self.reader.register(cx.waker());
                if self
                    .state
                    .compare_exchange(st, EXCH_EMPTY_FLUSHED, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    cx.waker().wake_by_ref();
                }
                self.writer.wake();
                return Poll::Pending;
            }
            EXCH_FULL => EXCH_EMPTY,
            EXCH_FULL_FLUSH => EXCH_EMPTY_FLUSH,
            EXCH_FULL_CLOSED => EXCH_DONE,
            EXCH_DONE => {
                cx.waker().wake_by_ref();
                return Poll::Ready(Ok(None));
            }
            // DROPPED
            _ => {
                return Poll::Ready(Err(IoError::InvalidState));
            }
        };

        // get item and switch to nextst
        if self
            .state
            .compare_exchange(st, nextst, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let item = guard.take();
        self.writer.wake();
        drop(guard);

        // An item (or the final EOS if nextst == DONE) was consumed, so
        // pre-wake the caller per the con_poll* contract.
        cx.waker().wake_by_ref();

        if let Some(item) = item {
            Poll::Ready(Ok(Some(item)))
        } else if nextst == EXCH_DONE {
            Poll::Ready(Ok(None))
        } else {
            // The state said FULL but the item slot was empty — shouldn't happen
            // in correct usage. Self-wake and pend so the caller retries.
            Poll::Pending
        }
    }

    fn drop_read(&self) {
        let mut guard = self.item.lock();
        let st = self.state.load(Ordering::Acquire);
        match st {
            EXCH_DROPPED | EXCH_DONE => (),
            _ => {
                // Move to DROPPED and discard any in-flight item.
                self.state.store(EXCH_DROPPED, Ordering::Release);
                *guard = None;
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
    fn prod_poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state.load(Ordering::Acquire);
        match st {
            // Any empty state means the slot is free.
            EXCH_EMPTY | EXCH_EMPTY_FLUSH | EXCH_EMPTY_FLUSHED => Poll::Ready(Ok(())),
            // Slot occupied — wait for the reader to consume.
            EXCH_FULL | EXCH_FULL_FLUSH | EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            EXCH_DROPPED => Poll::Ready(Err(IoError::BrokenPipe)),
            _ => Poll::Ready(Err(IoError::InvalidState)),
        }
    }

    /// Places an item into the slot if it is empty.
    ///
    /// Uses a compare-exchange on the state to atomically transition from
    /// an empty state to `FULL`. If the CAS fails (e.g. concurrent writer
    /// or reader-side state change), returns an error.
    fn prod_poll_send(
        &self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), IoError>> {
        if item.is_none() {
            return Poll::Ready(Ok(()));
        }
        let mut guard = self.item.lock();
        let st = self.state.load(Ordering::Acquire);
        match st {
            EXCH_EMPTY | EXCH_EMPTY_FLUSH | EXCH_EMPTY_FLUSHED => {
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    *guard = item.take();
                    self.reader.wake();
                    Poll::Ready(Ok(()))
                } else {
                    // CAS failure under our mutex shouldn't happen in proper
                    // single-producer usage; stay defensive and re-poll.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            EXCH_FULL | EXCH_FULL_FLUSH | EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            EXCH_DROPPED => Poll::Ready(Err(IoError::BrokenPipe)),
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
    ///
    /// The loop handles CAS retries if the state changes concurrently.
    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state.load(Ordering::Acquire);
        match st {
            // Already in a flush/close state — wait for reader progress.
            EXCH_EMPTY_FLUSH | EXCH_FULL_FLUSH | EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Reader has acknowledged the flush.
            EXCH_EMPTY_FLUSHED => Ok(()).into(),
            // Slot is empty — request a flush.
            EXCH_EMPTY => {
                self.writer.register(cx.waker());
                if self
                    .state
                    .compare_exchange(st, EXCH_EMPTY_FLUSH, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    // CAS failed — retry the loop.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Item still in flight — mark as flush-pending.
            EXCH_FULL => {
                self.writer.register(cx.waker());
                // Hold the lock to prevent the reader from consuming the
                // item between our load and our CAS.
                let _guard = self.item.lock();
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL_FLUSH, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    // CAS failed — retry the loop.
                    cx.waker().wake_by_ref();
                }
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
    ///
    /// The loop handles CAS retries.
    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        let st = self.state.load(Ordering::Acquire);
        match st {
            // Empty (any sub-state) — go straight to DONE.
            EXCH_EMPTY | EXCH_EMPTY_FLUSH | EXCH_EMPTY_FLUSHED => {
                if self
                    .state
                    .compare_exchange(st, EXCH_DONE, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                    Ok(()).into()
                } else {
                    // CAS failed — retry.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            // Item in flight — mark as "last item, then close".
            EXCH_FULL | EXCH_FULL_FLUSH => {
                self.writer.register(cx.waker());
                // Hold the lock to prevent the reader from consuming the
                // item between our load and our CAS.
                let _guard = self.item.lock();
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL_CLOSED, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    // CAS failed — retry.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Already waiting for the reader to take the last item.
            EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
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

    /// Helper: run a closure with a no-op waker context.
    fn with_noop_cx<T>(f: impl FnOnce(&mut Context<'_>) -> T) -> T {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        f(&mut cx)
    }

    #[test]
    fn send_receive_single_item() {
        let r = IoExchange::new();

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
        let r: IoExchange<i32> = IoExchange::new();

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
        let r = IoExchange::new();
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
        let r: IoExchange<i32> = IoExchange::new();

        let closed = with_noop_cx(|cx| r.prod_poll_close(cx));
        assert!(matches!(closed, Poll::Ready(Ok(()))));

        let end = with_noop_cx(|cx| r.con_poll_read(cx));
        assert!(matches!(end, Poll::Ready(Ok(None))));
    }

    #[test]
    fn close_after_full_delivers_last_item() {
        let r = IoExchange::new();
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
        let r: IoExchange<i32> = IoExchange::new();
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
        let r = IoExchange::<i32>::new();
        r.state.store(EXCH_DROPPED, AtomicOrdering::Release);

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
    fn con_poll_read_pre_wakes_on_consume_and_eof() {
        // Pre-wake when an item is consumed, and again when EOS is
        // consumed (each EOS return counts as consumption).
        let r = IoExchange::new();
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
        assert!(cw.count() > 0, "consuming EOS must pre-wake");
    }
}
