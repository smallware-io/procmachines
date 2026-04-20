//! Interior-mutable stream trait for async byte-stream consumption.
//!
//! This module defines [`IoReader`], a single-consumer polling trait for
//! reading [`Bytes`] from an asynchronous byte stream.  Unlike
//! `futures::AsyncRead`, the trait uses interior mutability (`&self`
//! receivers) so that both the reader and writer halves of a channel can
//! be accessed through a single shared reference.

use std::task::{Context, Poll};

use bytes::Bytes;

/// A single-consumer polling trait for reading bytes from an async stream.
///
/// All methods take `&self` (interior mutability), so the reader and its
/// paired writer can share a single allocation without splitting into
/// separate handles.
///
/// # `con_poll*` semantics
///
/// The `con_poll*` methods on this trait are intended for use by a single
/// "consumer" — a task that is consuming bytes (or end-of-stream signals)
/// that have been produced upstream.
///
/// - If a `con_poll*` method returns [`Poll::Pending`], the calling task is
///   registered to be woken later when the same call might return
///   [`Poll::Ready`].
/// - If a `con_poll*` method returns [`Poll::Ready`] **and the return
///   indicates that something was actually consumed**, the calling task is
///   **immediately pre-woken** so it will be re-polled after processing
///   the output.  This is the opposite of the `prod_poll*` contract on
///   [`IoWriter`](crate::io_writer::IoWriter): consumers are expected to
///   drain in a loop, so a consuming `Ready` keeps the task scheduled to
///   consume more.
/// - A `Poll::Ready` that does **not** consume anything — for example a
///   [`con_poll_read`](IoReader::con_poll_read) call with `max_len == 0`
///   on a live stream, which only probes "is there data?" — does not
///   pre-wake the caller.
///
/// End-of-stream — [`Poll::Ready(Ok(None))`](Poll::Ready) from
/// [`con_poll_read`](IoReader::con_poll_read) — is treated as consumption
/// for the purposes of this contract: each call consumes one repetition of
/// the EOS signal and pre-wakes the caller, even when `max_len == 0`.  The
/// signal is repeatable, so it is the caller's responsibility to recognise
/// end-of-stream and break out of any loop that would otherwise consume it
/// forever.
pub trait IoReader: Send {
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
    /// | `Poll::Ready(Ok(None))` | End-of-stream has been reached.  This signal is repeatable and each return is treated as consuming one repetition, so the caller is pre-woken.  The caller must recognise EOS and break its read loop. |
    /// | `Poll::Ready(Ok(Some(data)))` | Up to `max_len` bytes were consumed: `data.len() <= max_len`, and `data.len() >= 1` whenever `max_len > 0`.  If `max_len == 0` the call acts as a "is there data?" probe and `data.len()` will be 0. |
    /// | `Poll::Ready(Err(e))` | A stream error has occurred. |
    ///
    /// Per the `con_poll*` contract, a `Poll::Ready` return pre-wakes the
    /// calling task only when it indicates that something was actually
    /// consumed:
    ///
    /// - `Ready(Ok(Some(data)))` with `max_len > 0` — bytes were
    ///   consumed, pre-wake.
    /// - `Ready(Ok(None))` — one repetition of the EOS signal was
    ///   consumed, pre-wake.
    /// - `Ready(Ok(Some(empty)))` from a `max_len == 0` probe on a live
    ///   stream — nothing consumed, **no** pre-wake.
    fn con_poll_read(
        &self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<Bytes>>>;

    /// Signals that the reader is no longer interested in further bytes.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight data is discarded.
    fn drop_read(&self);
}
