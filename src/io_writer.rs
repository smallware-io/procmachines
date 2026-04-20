//! Interior-mutable sink trait for async byte delivery.
//!
//! This module defines [`IoWriter`], a single-producer polling trait for
//! sending [`Bytes`] into an asynchronous byte stream.  Unlike
//! `futures::AsyncWrite`, the trait uses interior mutability (`&self`
//! receivers) and transfers data via [`Bytes`], avoiding copies when the
//! underlying buffer is reference-counted.

use std::task::{Context, Poll};

use bytes::Bytes;

/// A single-producer polling trait for sending bytes into an async stream.
///
/// All methods take `&self` (interior mutability), so the writer and its
/// paired reader can share a single allocation without splitting into
/// separate handles.
///
/// # `prod_poll*` semantics
///
/// The `prod_poll*` methods on this trait are intended for use by a single
/// "producer" — a task that is producing bytes (or progress toward delivery)
/// to be consumed downstream.  Each `prod_poll*` method behaves like
/// [`Future::poll`](std::future::Future::poll):
///
/// - If it returns [`Poll::Pending`], the calling task is registered to be
///   woken later when the same call might return [`Poll::Ready`].
/// - Unlike other kinds of `poll*` methods (e.g. `watch_poll*`), the calling
///   task is **not** registered or pre-woken when a `prod_poll*` method
///   returns [`Poll::Ready`].  A `Ready` return is simply a signal that the
///   producer may proceed; it is the producer's responsibility to keep
///   calling until something blocks.
///
/// Producers are expected to produce everything they can without blocking.
/// Consequently, if no `prod_poll*` method ever returns [`Poll::Pending`],
/// the producer should eventually end up blocked on an input poll (such as
/// a `watch_poll*` or other upstream source) — unless it has finished its
/// task entirely.
///
/// Because these methods are single-producer, implementations typically
/// keep a single waker slot for the producing task.  Registering a new
/// waker may drop any previously registered waker without notification.
pub trait IoWriter: Send {
    /// The error type returned by write operations.
    type Error;

    /// Attempts to send data.
    ///
    /// - If `bytes` is empty, `Ready(Ok(0))` is returned immediately.
    /// - If the channel cannot accept data right now, `Poll::Pending` is
    ///   returned and the task is registered for wake-up.
    /// - Otherwise some prefix of `bytes` is consumed and sent.  The
    ///   number of bytes written (always > 0) is returned, and `bytes`
    ///   is advanced past the consumed portion.
    fn prod_poll_write(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<Result<usize, Self::Error>>;

    /// Requests that any buffered bytes be delivered, and checks progress.
    ///
    /// The definition of "flushed" is implementation-specific.  For
    /// [`IoBytesExchange`](super::io_bytes_exchange::IoBytesExchange),
    /// flushing completes when the reader has consumed the in-flight data
    /// and then performed a read that observes the empty slot (confirming
    /// it has seen all sent data).
    ///
    /// Returns `Poll::Ready(Ok(()))` once the flush is acknowledged.
    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Signals that no further bytes will be sent and waits for the close
    /// handshake to complete.
    ///
    /// After closing, the reader will eventually observe end-of-stream.
    /// If data is still in flight, the close is deferred until the reader
    /// consumes it.
    ///
    /// Returns `Poll::Ready(Ok(()))` once the close is fully acknowledged.
    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
