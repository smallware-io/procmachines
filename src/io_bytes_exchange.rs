//! Single-slot, lock-assisted async byte-stream exchange.
//!
//! [`IoBytesExchange`] is a bounded, single-producer / single-consumer
//! channel that transfers [`Bytes`] values one at a time through an
//! interior-mutable shared reference.  It implements both [`IoReader`]
//! (consumer side) and [`IoWriter`] (producer side).
//!
//! # Design
//!
//! The exchange holds at most one in-flight [`Bytes`] value.  An atomic
//! state byte ([`AtomicU8`]) encodes the current lifecycle phase, while a
//! [`Mutex<Bytes>`] protects the payload itself.  Two [`AtomicWaker`]s
//! (one per side) handle task wake-ups.
//!
//! The state machine has eight states (see the `EXCH_*` constants) that
//! track slot occupancy, flush handshaking, close sequencing, and reader
//! drop.  All transitions use compare-and-swap or are performed under the
//! data mutex to avoid races between the reader and writer.

use std::{
    sync::{
        Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

use crate::io_exchange::ExchangeWriteError;
use bytes::Bytes;
use futures::task::AtomicWaker;

use crate::{io_reader::IoReader, io_writer::IoWriter};

/// A single-slot, lock-assisted async byte-stream exchange.
///
/// Implements both [`IoWriter`] (producer/writer side) and [`IoReader`]
/// (consumer/reader side) using interior mutability, so both halves can
/// be accessed through a single shared `&IoBytesExchange` reference.
///
/// # Lifecycle
///
/// ```text
///  EMPTY ──write──▶ FULL ──read──▶ EMPTY  (normal data transfer)
///
///  *_EMPTY ─flush─▶ EMPTY_FLUSH ─reader sees─▶ EMPTY_FLUSHED
///                                                 │
///                                          writer sees ──▶ flush complete
///
///  EMPTY* ──close──▶ DONE         (immediate if slot empty)
///  FULL*  ──close──▶ FULL_CLOSED ─read─▶ DONE
///
///  any    ─drop_read─▶ DROPPED   (reader gives up)
/// ```
#[derive(Debug)]
pub struct IoBytesExchange {
    /// Waker for the reader side, notified when an item is placed or the
    /// stream is closed.
    reader: AtomicWaker,
    /// Waker for the writer side, notified when the reader consumes the
    /// item (freeing the slot) or drops.
    writer: AtomicWaker,
    /// Atomic state machine governing the exchange lifecycle.
    state: AtomicU8,
    /// Stored payload
    data: Mutex<Bytes>,
}

// ---------------------------------------------------------------------------
// State machine constants
// ---------------------------------------------------------------------------
//
// The lifecycle is encoded as a single `u8`.  States 0–2 represent an
// empty slot (no data in flight), states 3–5 represent a full slot
// (data waiting for the reader), and states 6–7 are terminal.
//
//  ┌───────────────── empty ──────────────────┐  ┌────── full ──────┐  ┌─ terminal ─┐
//  │  EMPTY  EMPTY_FLUSH  EMPTY_FLUSHED       │  │ FULL  FULL_FLUSH │  │ DONE       │
//  │   (0)      (1)           (2)             │  │  (3)     (4)     │  │  (6)       │
//  └──────────────────────────────────────────┘  │     FULL_CLOSED  │  │ DROPPED    │
//                                                │        (5)       │  │  (7)       │
//                                                └──────────────────┘  └────────────┘

/// Slot is empty, no flush requested.
const EXCH_EMPTY: u8 = 0;
/// Slot is empty; the writer has requested a flush but the reader has not
/// yet acknowledged it (i.e., the reader has not polled and observed the
/// empty slot since the flush was requested).
const EXCH_EMPTY_FLUSH: u8 = 1;
/// Slot is empty and the reader has acknowledged the flush by observing
/// the empty slot after the flush request.  The writer can now treat the
/// flush as complete.
const EXCH_EMPTY_FLUSHED: u8 = 2;
/// Slot contains data, no flush or close pending.
const EXCH_FULL: u8 = 3;
/// Slot contains data and a flush has been requested.  After the reader
/// consumes the data the state drops to `EMPTY_FLUSH`.
const EXCH_FULL_FLUSH: u8 = 4;
/// Slot contains the **final** chunk of data.  After the reader consumes
/// it the state transitions directly to `DONE`.
const EXCH_FULL_CLOSED: u8 = 5;
/// The stream is finished — the reader will see `None` from now on.
const EXCH_DONE: u8 = 6;
/// The reader called [`IoReader::drop_read`]; further writes will return
/// [`ExchangeWriteError::ReaderDropped`].
const EXCH_DROPPED: u8 = 7;

impl Default for IoBytesExchange {
    fn default() -> Self {
        Self::new()
    }
}

impl IoBytesExchange {
    /// Creates a new, empty exchange in the `EMPTY` state.
    pub fn new() -> Self {
        Self {
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            state: AtomicU8::new(EXCH_EMPTY),
            data: Mutex::new(Bytes::new()),
        }
    }

    /// Resets the exchange to its initial `EMPTY` state, discarding any
    /// in-flight data and waking any registered reader or writer so they
    /// re-poll and observe the fresh state.
    pub fn reset(&self) {
        let mut guard = self.data.lock().unwrap();
        self.state.store(EXCH_EMPTY, Ordering::SeqCst);
        *guard = Bytes::new();
        drop(guard);
        self.reader.wake();
        self.writer.wake();
    }
}

// ---------------------------------------------------------------------------
// IoReader (consumer / reader side)
// ---------------------------------------------------------------------------

impl IoReader for IoBytesExchange {
    fn con_poll_read(
        &self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<Bytes>>> {
        // Hold the data lock so state and payload stay in sync with the
        // writer during transitions that touch both.
        let mut guard = self.data.lock().unwrap();
        let st = self.state.load(Ordering::Acquire);

        let nextst = match st {
            // Already flushed; nothing to do until the writer sends more
            // data or closes.
            EXCH_EMPTY_FLUSHED => {
                self.reader.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                return Poll::Pending;
            }
            // Slot is empty; observing it acknowledges any pending flush.
            // Transition to EMPTY_FLUSHED (same move in both sub-cases).
            EXCH_EMPTY | EXCH_EMPTY_FLUSH => {
                self.reader.register(cx.waker());
                if self
                    .state
                    .compare_exchange(st, EXCH_EMPTY_FLUSHED, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    // Writer raced us (e.g. prod_poll_close); re-poll to
                    // re-evaluate.
                    cx.waker().wake_by_ref();
                }
                self.writer.wake();
                return Poll::Pending;
            }
            // Full states — map to the corresponding post-consume state.
            EXCH_FULL => EXCH_EMPTY,
            EXCH_FULL_FLUSH => EXCH_EMPTY_FLUSH,
            EXCH_FULL_CLOSED => EXCH_DONE,
            // Terminal (DONE or DROPPED) — end-of-stream, repeatable.
            _ => {
                // con_poll* contract: each EOS return counts as consuming
                // one repetition of the signal, so pre-wake the caller.
                // The caller must recognise EOS and break its loop.
                cx.waker().wake_by_ref();
                return Poll::Ready(Ok(None));
            }
        };

        // --- We are in a FULL* state and expect payload data. ---

        if guard.is_empty() {
            // Defensive: state claimed FULL but slot is empty.  Transition
            // to nextst and self-wake so the caller re-polls into the
            // correct branch.
            let _ = self
                .state
                .compare_exchange(st, nextst, Ordering::SeqCst, Ordering::SeqCst);
            self.writer.wake();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        if max_len == 0 {
            // The caller only wants to check liveness, not consume data.
            // Return an empty Bytes to signal "stream is alive".  Per the
            // con_poll* contract this non-consuming Ready does not pre-wake.
            return Poll::Ready(Ok(Some(Bytes::new())));
        }

        // Consume up to `max_len` bytes out of the slot.
        let mut data = Bytes::new();
        std::mem::swap(&mut data, &mut *guard);

        if data.len() > max_len {
            // Partial read — put the remainder back, stay in FULL* state.
            *guard = data.split_off(max_len);
        } else {
            // All data consumed — advance to nextst.
            self.state.store(nextst, Ordering::Release);
        }
        // The slot may now have room (or the stream is done); notify the
        // writer either way.
        self.writer.wake();
        drop(guard);

        // con_poll* contract: bytes were actually consumed, so pre-wake
        // the caller to keep it scheduled to drain more.
        cx.waker().wake_by_ref();
        Poll::Ready(Ok(Some(data)))
    }

    fn drop_read(&self) {
        let mut guard = self.data.lock().unwrap();
        let st = self.state.load(Ordering::Acquire);
        match st {
            EXCH_DROPPED | EXCH_DONE => (),
            _ => {
                // Force-terminate: discard any in-flight data and move
                // to the DROPPED terminal state.
                self.state.store(EXCH_DROPPED, Ordering::Release);
                *guard = Bytes::new();
                self.writer.wake();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IoWriter (producer / writer side)
// ---------------------------------------------------------------------------

impl IoWriter for IoBytesExchange {
    type Error = ExchangeWriteError;

    /// Places data into the exchange slot.
    ///
    /// The entire contents of `data` are moved into the slot in one shot
    /// (the slot can hold an arbitrarily large [`Bytes`]).  On success the
    /// `data` handle is left empty and the byte count is returned.
    fn prod_poll_write(
        &self,
        cx: &mut Context<'_>,
        data: &mut Bytes,
    ) -> Poll<Result<usize, ExchangeWriteError>> {
        // Zero-byte writes are a no-op.  Per prod_poll* semantics: no
        // registration, no pre-wake.
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut guard = self.data.lock().unwrap();
        let st = self.state.load(Ordering::Acquire);
        match st {
            // Any empty sub-state → place data and move to FULL.
            EXCH_EMPTY | EXCH_EMPTY_FLUSH | EXCH_EMPTY_FLUSHED => {
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let sz = data.len();
                    let mut t = Bytes::new();
                    std::mem::swap(&mut t, data);
                    *guard = t;
                    self.reader.wake();
                    Poll::Ready(Ok(sz))
                } else {
                    // CAS failure under our mutex shouldn't happen in
                    // single-producer usage; stay defensive and re-poll.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            // Slot is occupied — back-pressure.
            EXCH_FULL | EXCH_FULL_FLUSH | EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Reader is gone.
            EXCH_DROPPED => Poll::Ready(Err(ExchangeWriteError::ReaderDropped)),
            // DONE or any unexpected value.
            _ => Poll::Ready(Err(ExchangeWriteError::InvalidState)),
        }
    }

    /// Requests that the reader acknowledge all previously sent data.
    ///
    /// The flush is a two-phase handshake:
    ///
    /// 1. The writer transitions to a `*_FLUSH` state and wakes the reader.
    /// 2. The reader eventually observes the empty slot and transitions to
    ///    `EMPTY_FLUSHED`.
    /// 3. The writer sees `EMPTY_FLUSHED` and returns `Ready(Ok(()))`.
    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), ExchangeWriteError>> {
        let st = self.state.load(Ordering::Acquire);
        match st {
            // A flush or close is already in progress — wait for the
            // reader to make progress.
            EXCH_EMPTY_FLUSH | EXCH_FULL_FLUSH | EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Reader has acknowledged the flush.
            EXCH_EMPTY_FLUSHED => Poll::Ready(Ok(())),
            // Slot empty, no flush yet — request one.
            EXCH_EMPTY => {
                self.writer.register(cx.waker());
                if self
                    .state
                    .compare_exchange(st, EXCH_EMPTY_FLUSH, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    // CAS failed — state raced (e.g. drop_read); re-poll.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Data still in flight — tag the flush onto it.
            EXCH_FULL => {
                self.writer.register(cx.waker());
                // Hold the data lock so the reader cannot consume the
                // payload (and change the state) between our load and CAS.
                let _guard = self.data.lock().unwrap();
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL_FLUSH, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Terminal (DONE or DROPPED) — nothing left to flush.
            _ => Poll::Ready(Ok(())),
        }
    }

    /// Signals that no more data will be sent and waits for acknowledgement.
    ///
    /// If the slot is empty the exchange moves directly to `DONE`.  If data
    /// is still in flight the state becomes `FULL_CLOSED`, allowing the
    /// reader to consume the last chunk before seeing end-of-stream.
    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), ExchangeWriteError>> {
        let st = self.state.load(Ordering::Acquire);
        match st {
            // Any empty sub-state — go straight to DONE.
            EXCH_EMPTY | EXCH_EMPTY_FLUSH | EXCH_EMPTY_FLUSHED => {
                if self
                    .state
                    .compare_exchange(st, EXCH_DONE, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                    Poll::Ready(Ok(()))
                } else {
                    // CAS raced (e.g. drop_read); re-poll.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            // Data in flight — mark as "last chunk, then close".
            EXCH_FULL | EXCH_FULL_FLUSH => {
                self.writer.register(cx.waker());
                // Hold the data lock so the reader cannot consume the
                // payload (and change the state) between our load and CAS.
                let _guard = self.data.lock().unwrap();
                if self
                    .state
                    .compare_exchange(st, EXCH_FULL_CLOSED, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.reader.wake();
                } else {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Already waiting for the reader to take the last chunk.
            EXCH_FULL_CLOSED => {
                self.writer.register(cx.waker());
                if st != self.state.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            // Terminal (DONE or DROPPED) — already finished.
            _ => Poll::Ready(Ok(())),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake};

    // -----------------------------------------------------------------------
    // Test waker infrastructure
    // -----------------------------------------------------------------------

    /// A minimal `Waker` that records how many times it was woken.
    struct CountWaker(std::sync::atomic::AtomicUsize);

    impl CountWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self(std::sync::atomic::AtomicUsize::new(0)))
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

    /// Create a `CountWaker` and its corresponding `std::task::Waker`.
    ///
    /// The caller should construct a `Context` locally via
    /// `Context::from_waker(&waker)` — this avoids lifetime issues that
    /// arise when trying to return a `Context` from a helper function.
    fn make_waker() -> (Arc<CountWaker>, std::task::Waker) {
        let cw = CountWaker::new();
        let w: std::task::Waker = cw.clone().into();
        (cw, w)
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_exchange_starts_empty() {
        let ex = IoBytesExchange::new();
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY);
        assert!(ex.data.lock().unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Basic write → read round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn write_then_read() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        let mut payload = Bytes::from_static(b"hello");
        let n = match IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("expected Ready(Ok(_)), got {:?}", other),
        };
        assert_eq!(n, 5);
        assert!(payload.is_empty());
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);

        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"hello"),
            other => panic!("expected Ready(Ok(Some(hello))), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY);
    }

    // -----------------------------------------------------------------------
    // Zero-length reads and writes
    // -----------------------------------------------------------------------

    #[test]
    fn write_empty_bytes_is_noop() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let mut empty = Bytes::new();
        match IoWriter::prod_poll_write(&ex, &mut wcx, &mut empty) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("expected Ready(Ok(0)), got {:?}", other),
        }
        // State should still be EMPTY — nothing was sent.
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY);
    }

    #[test]
    fn read_zero_on_empty_returns_pending() {
        let ex = IoBytesExchange::new();
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // max_len=0 on an empty exchange should return Pending (no data,
        // and we can't say the stream is in error).
        match IoReader::con_poll_read(&ex, &mut rcx, 0) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
    }

    #[test]
    fn read_zero_on_full_returns_some_empty() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (rw, rwaker) = make_waker();
        let mut rcx = Context::from_waker(&rwaker);

        let mut payload = Bytes::from_static(b"data");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);

        rw.reset();
        // max_len=0 on a full exchange: "stream is alive" → Some(empty).
        match IoReader::con_poll_read(&ex, &mut rcx, 0) {
            Poll::Ready(Ok(Some(data))) => assert!(data.is_empty()),
            other => panic!("expected Ready(Ok(Some(empty))), got {:?}", other),
        }
        // Data should still be in the slot (not consumed).
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);
        // con_poll* contract: a non-consuming Ready (max_len == 0 probe on
        // a live stream) must not pre-wake the caller.
        assert_eq!(rw.count(), 0, "max_len=0 probe must not pre-wake");
    }

    // -----------------------------------------------------------------------
    // Partial reads
    // -----------------------------------------------------------------------

    #[test]
    fn partial_read_splits_data() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (rw, rwaker) = make_waker();
        let mut rcx = Context::from_waker(&rwaker);

        let mut payload = Bytes::from_static(b"abcdef");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);

        // Read only 3 bytes.
        rw.reset();
        match IoReader::con_poll_read(&ex, &mut rcx, 3) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"abc"),
            other => panic!("expected 'abc', got {:?}", other),
        }
        // State should still be FULL (remainder in slot).
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);
        // Reader should have been pre-emptively woken (more data available).
        assert!(
            rw.count() > 0,
            "reader waker should fire after partial read"
        );

        // Read the rest.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"def"),
            other => panic!("expected 'def', got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY);
    }

    // -----------------------------------------------------------------------
    // Back-pressure: write when full returns Pending
    // -----------------------------------------------------------------------

    #[test]
    fn write_when_full_returns_pending() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let mut a = Bytes::from_static(b"first");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut a);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);

        let mut b = Bytes::from_static(b"second");
        match IoWriter::prod_poll_write(&ex, &mut wcx, &mut b) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Read on empty returns Pending
    // -----------------------------------------------------------------------

    #[test]
    fn read_on_empty_returns_pending() {
        let ex = IoBytesExchange::new();
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Waker notifications
    // -----------------------------------------------------------------------

    #[test]
    fn writer_is_woken_after_read_frees_slot() {
        let ex = IoBytesExchange::new();
        let (ww, wwaker) = make_waker();
        let mut wcx = Context::from_waker(&wwaker);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        let mut payload = Bytes::from_static(b"x");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);

        // Second write should Pend and register the writer waker.
        ww.reset();
        let mut more = Bytes::from_static(b"y");
        assert!(matches!(
            IoWriter::prod_poll_write(&ex, &mut wcx, &mut more),
            Poll::Pending
        ));

        // Reader consumes → writer should be woken.
        ww.reset();
        let _ = IoReader::con_poll_read(&ex, &mut rcx, 1024);
        assert!(ww.count() > 0, "writer should be woken when slot is freed");
    }

    #[test]
    fn reader_is_woken_after_write_fills_slot() {
        let ex = IoBytesExchange::new();
        let (rw, rwaker) = make_waker();
        let mut rcx = Context::from_waker(&rwaker);
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        // Reader tries to read an empty slot.
        rw.reset();
        assert!(matches!(
            IoReader::con_poll_read(&ex, &mut rcx, 1024),
            Poll::Pending
        ));

        // Writer places data → reader should be woken.
        rw.reset();
        let mut payload = Bytes::from_static(b"wake-up");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        assert!(rw.count() > 0, "reader should be woken when data arrives");
    }

    // -----------------------------------------------------------------------
    // Flush handshake
    // -----------------------------------------------------------------------

    #[test]
    fn flush_on_empty_completes_after_reader_ack() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Flush on an empty slot: writer requests flush.
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSH);

        // Reader polls and acknowledges the flush.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Pending => {}
            other => panic!("expected Pending (ack), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSHED);

        // Writer sees the acknowledgement.
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
    }

    #[test]
    fn flush_on_full_completes_after_read_and_ack() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Write data, then flush.
        let mut payload = Bytes::from_static(b"flush-me");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_FLUSH);

        // Reader consumes the data → state becomes EMPTY_FLUSH.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"flush-me"),
            other => panic!("expected data, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSH);

        // Reader polls again → acknowledges the flush.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Pending => {}
            other => panic!("expected Pending (ack), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSHED);

        // Writer sees flush complete.
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
    }

    #[test]
    fn flush_already_flushed_returns_ready() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Get to EMPTY_FLUSHED via the normal path.
        let _ = IoWriter::prod_poll_flush(&ex, &mut wcx); // EMPTY → EMPTY_FLUSH
        let _ = IoReader::con_poll_read(&ex, &mut rcx, 1024); // EMPTY_FLUSH → EMPTY_FLUSHED

        // A second flush should return Ready immediately.
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected immediate Ready(Ok(())), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Close handshake
    // -----------------------------------------------------------------------

    #[test]
    fn close_on_empty_goes_to_done() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);

        // Reader sees EOF.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected Ready(Ok(None)), got {:?}", other),
        }
    }

    #[test]
    fn close_on_full_delivers_last_item_then_eof() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Write data then close.
        let mut payload = Bytes::from_static(b"last");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_CLOSED);

        // Reader gets the last item.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"last"),
            other => panic!("expected data, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);

        // Subsequent read returns EOF.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF, got {:?}", other),
        }
    }

    #[test]
    fn close_is_idempotent() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);

        // Closing again should still return Ok.
        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Reader drop
    // -----------------------------------------------------------------------

    #[test]
    fn drop_read_signals_writer() {
        let ex = IoBytesExchange::new();
        let (ww, wwaker) = make_waker();
        let mut wcx = Context::from_waker(&wwaker);

        // Fill the slot so a second write has to block (Pending path
        // registers the writer's waker, per the prod_poll* contract).
        let mut payload = Bytes::from_static(b"first");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        let mut more = Bytes::from_static(b"second");
        assert!(matches!(
            IoWriter::prod_poll_write(&ex, &mut wcx, &mut more),
            Poll::Pending
        ));

        // Reader drops — the registered writer waker should fire.
        ww.reset();
        IoReader::drop_read(&ex);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DROPPED);
        // In-flight data should be discarded.
        assert!(ex.data.lock().unwrap().is_empty());
        assert!(ww.count() > 0, "writer should be woken on reader drop");

        // Subsequent write returns ReaderDropped.
        let mut more = Bytes::from_static(b"nope");
        match IoWriter::prod_poll_write(&ex, &mut wcx, &mut more) {
            Poll::Ready(Err(ExchangeWriteError::ReaderDropped)) => {}
            other => panic!("expected ReaderDropped, got {:?}", other),
        }
    }

    #[test]
    fn drop_read_on_empty() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        IoReader::drop_read(&ex);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DROPPED);

        let mut data = Bytes::from_static(b"x");
        match IoWriter::prod_poll_write(&ex, &mut wcx, &mut data) {
            Poll::Ready(Err(ExchangeWriteError::ReaderDropped)) => {}
            other => panic!("expected ReaderDropped, got {:?}", other),
        }
    }

    #[test]
    fn drop_read_is_idempotent() {
        let ex = IoBytesExchange::new();
        IoReader::drop_read(&ex);
        IoReader::drop_read(&ex); // should not panic
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DROPPED);
    }

    #[test]
    fn drop_read_after_done_is_noop() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);

        IoReader::drop_read(&ex);
        // Should stay DONE, not change to DROPPED.
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);
    }

    // -----------------------------------------------------------------------
    // Read after terminal states
    // -----------------------------------------------------------------------

    #[test]
    fn read_after_done_returns_none() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);

        for _ in 0..3 {
            match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
                Poll::Ready(Ok(None)) => {}
                other => panic!("expected None, got {:?}", other),
            }
        }
    }

    #[test]
    fn read_after_dropped_returns_none() {
        let ex = IoBytesExchange::new();
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        IoReader::drop_read(&ex);

        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected None, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Write after terminal states
    // -----------------------------------------------------------------------

    #[test]
    fn write_after_done_returns_error() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);

        let mut data = Bytes::from_static(b"too late");
        match IoWriter::prod_poll_write(&ex, &mut wcx, &mut data) {
            Poll::Ready(Err(_)) => {}
            other => panic!("expected error after DONE, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Flush / close on terminal states
    // -----------------------------------------------------------------------

    #[test]
    fn flush_after_done_returns_ok() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);

        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ok after DONE, got {:?}", other),
        }
    }

    #[test]
    fn flush_after_dropped_returns_ok() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        IoReader::drop_read(&ex);

        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ok after DROPPED, got {:?}", other),
        }
    }

    #[test]
    fn close_after_dropped_returns_ok() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        IoReader::drop_read(&ex);

        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ok after DROPPED, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Close supersedes flush
    // -----------------------------------------------------------------------

    #[test]
    fn close_on_full_flush_transitions_to_full_closed() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Write + flush → FULL_FLUSH.
        let mut payload = Bytes::from_static(b"data");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        let _ = IoWriter::prod_poll_flush(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_FLUSH);

        // Close should supersede the flush.
        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_CLOSED);

        // Reader gets the data.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"data"),
            other => panic!("expected data, got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);
    }

    // -----------------------------------------------------------------------
    // Multiple write-read cycles
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_write_read_cycles() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        for i in 0u32..10 {
            let msg = format!("msg-{}", i);
            let mut payload = Bytes::from(msg.clone());
            match IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload) {
                Poll::Ready(Ok(n)) => assert_eq!(n, msg.len()),
                other => panic!("cycle {}: write failed: {:?}", i, other),
            }

            match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
                Poll::Ready(Ok(Some(data))) => assert_eq!(data, Bytes::from(msg)),
                other => panic!("cycle {}: read failed: {:?}", i, other),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Partial reads across multiple polls
    // -----------------------------------------------------------------------

    #[test]
    fn partial_reads_drain_slot_correctly() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        let mut payload = Bytes::from_static(b"0123456789");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);

        // Read 4 at a time.
        match IoReader::con_poll_read(&ex, &mut rcx, 4) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"0123"),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);

        match IoReader::con_poll_read(&ex, &mut rcx, 4) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"4567"),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL);

        // Last 2 bytes — less than max_len, so slot becomes empty.
        match IoReader::con_poll_read(&ex, &mut rcx, 4) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"89"),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY);
    }

    // -----------------------------------------------------------------------
    // Partial read preserves close semantics
    // -----------------------------------------------------------------------

    #[test]
    fn partial_read_with_close_defers_done() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        let mut payload = Bytes::from_static(b"abcd");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_CLOSED);

        // Partial read — slot stays FULL_CLOSED because data remains.
        match IoReader::con_poll_read(&ex, &mut rcx, 2) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"ab"),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_CLOSED);

        // Drain the rest — now it should transition to DONE.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(Some(data))) => assert_eq!(&data[..], b"cd"),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);

        // EOF.
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected EOF, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Reader pre-emptive wakeup on EOF
    // -----------------------------------------------------------------------

    #[test]
    fn eof_with_max_len_gt_zero_wakes_reader() {
        let ex = IoBytesExchange::new();
        let (rw, rwaker) = make_waker();
        let mut rcx = Context::from_waker(&rwaker);
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);

        rw.reset();
        match IoReader::con_poll_read(&ex, &mut rcx, 1024) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected None, got {:?}", other),
        }
        // The reader should pre-emptively wake itself for consumption
        // requests on terminal states.
        assert!(
            rw.count() > 0,
            "reader should self-wake on EOF consumption request"
        );
    }

    #[test]
    fn eof_with_max_len_zero_pre_wakes_reader() {
        let ex = IoBytesExchange::new();
        let (rw, rwaker) = make_waker();
        let mut rcx = Context::from_waker(&rwaker);
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let _ = IoWriter::prod_poll_close(&ex, &mut wcx);

        rw.reset();
        match IoReader::con_poll_read(&ex, &mut rcx, 0) {
            Poll::Ready(Ok(None)) => {}
            other => panic!("expected None, got {:?}", other),
        }
        // con_poll* contract: each EOS return counts as consumption, so
        // it pre-wakes even with max_len == 0.  The caller is responsible
        // for recognising the signal and breaking out of its loop.
        assert!(
            rw.count() > 0,
            "reader should self-wake on EOF regardless of max_len"
        );
    }

    // -----------------------------------------------------------------------
    // Close on EMPTY_FLUSH / EMPTY_FLUSHED
    // -----------------------------------------------------------------------

    #[test]
    fn close_on_empty_flush_goes_to_done() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        // Get to EMPTY_FLUSH.
        let _ = IoWriter::prod_poll_flush(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSH);

        // Close should transition directly to DONE.
        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);
    }

    #[test]
    fn close_on_empty_flushed_goes_to_done() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);
        let (_, rw) = make_waker();
        let mut rcx = Context::from_waker(&rw);

        // Get to EMPTY_FLUSHED.
        let _ = IoWriter::prod_poll_flush(&ex, &mut wcx);
        let _ = IoReader::con_poll_read(&ex, &mut rcx, 1024);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_EMPTY_FLUSHED);

        match IoWriter::prod_poll_close(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DONE);
    }

    // -----------------------------------------------------------------------
    // Drop during flush
    // -----------------------------------------------------------------------

    #[test]
    fn drop_read_during_flush() {
        let ex = IoBytesExchange::new();
        let (_, ww) = make_waker();
        let mut wcx = Context::from_waker(&ww);

        let mut payload = Bytes::from_static(b"data");
        let _ = IoWriter::prod_poll_write(&ex, &mut wcx, &mut payload);
        let _ = IoWriter::prod_poll_flush(&ex, &mut wcx);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_FULL_FLUSH);

        IoReader::drop_read(&ex);
        assert_eq!(ex.state.load(Ordering::SeqCst), EXCH_DROPPED);

        // Flush should now return Ok (terminal state).
        match IoWriter::prod_poll_flush(&ex, &mut wcx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected Ok after drop, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Async integration test using tokio
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn async_write_read_roundtrip() {
        let ex = Arc::new(IoBytesExchange::new());

        let writer_ex = ex.clone();
        let writer = tokio::spawn(async move {
            std::future::poll_fn(|cx| {
                let mut data = Bytes::from_static(b"async-hello");
                IoWriter::prod_poll_write(&*writer_ex, cx, &mut data)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();

            std::future::poll_fn(|cx| {
                IoWriter::prod_poll_close(&*writer_ex, cx)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();
        });

        let reader_ex = ex.clone();
        let reader = tokio::spawn(async move {
            let data = std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                .await
                .unwrap();
            assert_eq!(data.unwrap(), Bytes::from_static(b"async-hello"));

            let eof = std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                .await
                .unwrap();
            assert!(eof.is_none(), "expected EOF");
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn async_multiple_chunks() {
        let ex = Arc::new(IoBytesExchange::new());
        let chunks: Vec<&[u8]> = vec![b"one", b"two", b"three"];

        let writer_ex = ex.clone();
        let writer = tokio::spawn(async move {
            for chunk in &chunks {
                std::future::poll_fn(|cx| {
                    let mut data = Bytes::from_static(chunk);
                    IoWriter::prod_poll_write(&*writer_ex, cx, &mut data)
                        .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
                })
                .await
                .unwrap();
            }
            std::future::poll_fn(|cx| {
                IoWriter::prod_poll_close(&*writer_ex, cx)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();
        });

        let reader_ex = ex.clone();
        let reader = tokio::spawn(async move {
            let mut received = Vec::new();
            loop {
                let result =
                    std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                        .await
                        .unwrap();
                match result {
                    Some(data) => received.push(data),
                    None => break,
                }
            }
            let all: Vec<&[u8]> = received.iter().map(|b| &b[..]).collect();
            assert_eq!(all, vec![b"one" as &[u8], b"two", b"three"]);
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn async_flush_handshake() {
        let ex = Arc::new(IoBytesExchange::new());

        let writer_ex = ex.clone();
        let writer = tokio::spawn(async move {
            // Send data.
            std::future::poll_fn(|cx| {
                let mut data = Bytes::from_static(b"flush-test");
                IoWriter::prod_poll_write(&*writer_ex, cx, &mut data)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();

            // Flush — blocks until reader acks.
            std::future::poll_fn(|cx| {
                IoWriter::prod_poll_flush(&*writer_ex, cx)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();

            // Send more after flush.
            std::future::poll_fn(|cx| {
                let mut data = Bytes::from_static(b"post-flush");
                IoWriter::prod_poll_write(&*writer_ex, cx, &mut data)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();

            std::future::poll_fn(|cx| {
                IoWriter::prod_poll_close(&*writer_ex, cx)
                    .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())))
            })
            .await
            .unwrap();
        });

        let reader_ex = ex.clone();
        let reader = tokio::spawn(async move {
            // Read first chunk.
            let data = std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&data[..], b"flush-test");

            // Read second chunk (written after flush completed).
            let data = std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&data[..], b"post-flush");

            // EOF.
            let eof = std::future::poll_fn(|cx| IoReader::con_poll_read(&*reader_ex, cx, 1024))
                .await
                .unwrap();
            assert!(eof.is_none());
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }
}
