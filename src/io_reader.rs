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
/// - If a `con_poll*` method returns [`Poll::Ready`], it indicates that
///   something has been "consumed", and the calling task is **immediately
///   pre-woken** so it will be re-polled after processing the output.  This
///   is the opposite of the `prod_poll*` contract on
///   [`IoWriter`](crate::io_writer::IoWriter): consumers are expected to
///   drain in a loop, so a `Ready` return keeps the task scheduled to
///   consume more.
///
/// End-of-stream signals (e.g. [`Poll::Ready(Ok(None))`](Poll::Ready) from
/// [`con_poll_read`](IoReader::con_poll_read)) can be consumed many times —
/// each call will continue to return the end-of-stream signal and continue
/// to pre-wake the caller.  It is the caller's responsibility to recognize
/// these signals and break out of any loop that would otherwise consume
/// them forever.
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
    /// | `Poll::Ready(Ok(None))` | End-of-stream has been reached.  This signal is repeatable — callers must recognize it and break their read loop. |
    /// | `Poll::Ready(Ok(Some(data)))` | Up to `max_len` bytes were consumed: `data.len() <= max_len`, and `data.len() >= 1` whenever `max_len > 0`.  If `max_len == 0` the call acts as a "is there data?" probe and `data.len()` will be 0. |
    /// | `Poll::Ready(Err(e))` | A stream error has occurred. |
    ///
    /// Per the `con_poll*` contract, every `Poll::Ready` return — including
    /// repeated end-of-stream or empty-probe results — pre-wakes the calling
    /// task.  The caller is responsible for breaking any loop that would
    /// otherwise spin on a non-consuming `Ready` (EOS, error, or an empty
    /// `max_len == 0` probe).
    fn con_poll_read(&self, cx: &mut Context<'_>, max_len: usize) -> Poll<std::io::Result<Option<Bytes>>>;

    /// Signals that the reader is no longer interested in further bytes.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight data is discarded.
    fn drop_read(&self);
}
