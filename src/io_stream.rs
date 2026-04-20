//! Interior-mutable stream trait for async item consumption.
//!
//! [`IoStream`] is analogous to [`futures::Stream`], but all methods take
//! `&self` instead of `Pin<&mut Self>`. This makes it usable through shared
//! references, matching the design of [`IoSink`](crate::io_sink::IoSink).
//!
//! The trait is implemented by [`IoExchange`](crate::io_exchange::IoExchange),
//! which provides the reader half of a single-slot rendezvous channel.

use std::task::{Context, Poll};

/// A `Stream`-like trait with interior mutability (`&self` receivers).
///
/// Provides async item consumption through shared references. Paired with
/// [`IoSink`](crate::io_sink::IoSink) to form a full duplex channel.
///
/// # `con_poll*` semantics
///
/// The `con_poll*` methods on this trait are intended for use by a single
/// "consumer" — a task that is consuming items (or end-of-stream signals)
/// that have been produced upstream.
///
/// - If a `con_poll*` method returns [`Poll::Pending`], the calling task is
///   registered to be woken later when the same call might return
///   [`Poll::Ready`].
/// - If a `con_poll*` method returns [`Poll::Ready`], it indicates that
///   something has been "consumed", and the calling task is **immediately
///   pre-woken** so it will be re-polled after processing the output.  This
///   is the opposite of the `prod_poll*` contract on
///   [`IoSink`](crate::io_sink::IoSink): consumers are expected to drain in
///   a loop, so a `Ready` return keeps the task scheduled to consume more.
///
/// End-of-stream signals (e.g. [`Poll::Ready(None)`](Poll::Ready) from
/// [`con_poll_read`](IoStream::con_poll_read), or
/// [`Poll::Ready(false)`](Poll::Ready) from
/// [`con_check_read`](IoStream::con_check_read)) can be consumed many times
/// — each call will continue to return the end-of-stream signal and
/// continue to pre-wake the caller.  It is the caller's responsibility to
/// recognize these signals and break out of any loop that would otherwise
/// consume them forever.
pub trait IoStream<ITEM> {
    /// Checks whether an item is available without consuming it.
    ///
    /// Returns:
    /// - `Poll::Ready(true)` — an item is available; a subsequent
    ///   [`con_poll_read`](IoStream::con_poll_read) would return `Poll::Ready(Some(...))`.
    /// - `Poll::Ready(false)` — the stream is finished; a subsequent
    ///   `con_poll_read` would return `Poll::Ready(None)`.
    /// - `Poll::Pending` — no item is available yet; the waker will be
    ///   notified when the state changes.
    fn con_check_read(&self, cx: &mut Context<'_>) -> Poll<bool>;

    /// Attempts to read the next item from the stream.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(item))` — an item was consumed.
    /// - `Poll::Ready(None)` — the stream is finished (writer closed).
    /// - `Poll::Pending` — no item is available yet.  The waker will be notified
    ///   when the state changes.
    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Option<ITEM>>;

    /// Signals that the reader is no longer interested in further items.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight item is discarded.
    fn drop_read(&self);
}
