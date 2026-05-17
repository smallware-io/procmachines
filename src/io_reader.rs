//! Single-consumer stream trait for async byte-stream consumption.
//!
//! This module defines [`IoReader`], a single-consumer polling trait for
//! reading [`Bytes`] from an asynchronous byte stream.  Like
//! `futures::AsyncRead`, the trait uses `&mut self` receivers, but it
//! avoids `Pin` and yields data as [`Bytes`] so consumers can hold
//! reference-counted slices into the underlying buffer.

use core::task::{Context, Poll};

use bytes::Bytes;

/// A single-consumer polling trait for reading bytes from an async stream.
///
/// All methods take `&mut self`.  An implementation that also implements
/// [`IoWriter`](crate::io_writer::IoWriter) on the same type (e.g.
/// [`IoBytesExchange`](super::io_bytes_exchange::IoBytesExchange)) requires
/// the caller to arrange exclusive access to each half — typically by
/// wrapping the shared object in a lock or by splitting it into separate
/// handles before handing the halves to producer and consumer tasks.
///
/// # `con_poll*` semantics
///
/// The `con_poll*` methods on this trait are intended for use by a single
/// "consumer" — a task that is consuming bytes (or end-of-stream signals)
/// that have been produced upstream.
///
/// The unifying rule: **the calling task is registered to be woken
/// whenever calling the same method again might return a different
/// result.**
///
/// - If a `con_poll*` method returns [`Poll::Pending`], the calling task
///   is registered to be woken later when the same call might return
///   [`Poll::Ready`] — the standard `poll` contract.
/// - If a `con_poll*` method returns [`Poll::Ready`] **and consumes
///   data**, the calling task is **pre-woken**, because the next call
///   cannot return the same data.  Consumers are expected to drain in a
///   loop, so a consuming `Ready` keeps the task scheduled.
/// - If a `con_poll*` method returns a terminal [`Poll::Ready`] —
///   end-of-stream ([`Ready(Ok(None))`](Poll::Ready)) or a persistent
///   error that the next call would re-emit — the caller is **not**
///   pre-woken: the same call would just return the same value again.
/// - A non-consuming `Poll::Ready` — for example a
///   [`con_poll_read`](IoReader::con_poll_read) call with `max_len == 0`
///   on a live stream, which only probes "is there data?" — does not
///   pre-wake the caller either; nothing changed.
pub trait IoReader: Send {
    type Error;
    /// Attempts to read the next chunk of bytes from the stream.
    ///
    /// This is the primary read primitive.  The caller specifies the
    /// maximum number of bytes it is willing to accept via `max_len`.
    ///
    /// # Return values
    ///
    /// | Condition | Meaning |
    /// |-----------|---------|
    /// | `Poll::Pending` | No data available yet; per the `con_poll*` contract the task is registered to be woken when that may change. |
    /// | `Poll::Ready(Ok(None))` | End-of-stream has been reached.  The signal is repeatable; subsequent calls return the same value, so the caller is **not** pre-woken.  The caller must recognise EOS and break its read loop. |
    /// | `Poll::Ready(Ok(Some(data)))` | Up to `max_len` bytes were consumed: `data.len() <= max_len`, and `data.len() >= 1` whenever `max_len > 0`.  If `max_len == 0` the call acts as a "is there data?" probe and `data.len()` will be 0. |
    /// | `Poll::Ready(Err(e))` | A stream error has occurred. |
    ///
    /// Per the `con_poll*` contract, a `Poll::Ready` return pre-wakes the
    /// calling task only when the next call could return a different
    /// result:
    ///
    /// - `Ready(Ok(Some(data)))` with `max_len > 0` — bytes were
    ///   consumed, pre-wake.
    /// - `Ready(Ok(None))` — end-of-stream, terminal: **no** pre-wake.
    /// - `Ready(Ok(Some(empty)))` from a `max_len == 0` probe on a live
    ///   stream — nothing consumed, **no** pre-wake.
    fn con_poll_read(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<Result<Option<Bytes>, Self::Error>>;

    /// Signals that the reader is no longer interested in further bytes.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight data is discarded.
    fn drop_read(&mut self);
}
