//! Interior-mutable stream trait for async item consumption.
//!
//! [`IoStream`] is analogous to [`futures::Stream`], but all methods take
//! `&self` instead of `Pin<&mut Self>`. This makes it usable through shared
//! references, matching the design of [`IoSink`](crate::io_sink::IoSink).
//!
//! The trait is implemented by [`IoExchange`](crate::io_exchange::IoExchange),
//! which provides the reader half of a single-slot rendezvous channel.

use core::{
    cell::RefCell,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::RefLockable;

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
/// - If a `con_poll*` method returns [`Poll::Ready`] **and the return value
///   indicates that something was actually consumed** (a non-idempotent
///   result, e.g. an item), the calling task is **immediately pre-woken** so
///   it will be re-polled after processing the output.  This is the opposite
///   of the `prod_poll*` contract on [`IoSink`](crate::io_sink::IoSink):
///   consumers are expected to drain in a loop, so a consuming `Ready` keeps
///   the task scheduled to consume more.
/// - A `Poll::Ready` that does **not** consume anything is idempotent and does
///   **not** pre-wake.  In particular, end-of-stream
///   ([`Poll::Ready(Ok(None))`](Poll::Ready)) is idempotent: repeated calls
///   keep returning end-of-stream, so it never pre-wakes (doing so would spin
///   the task forever).  It is the caller's responsibility to recognise
///   end-of-stream and break out of its read loop.
pub trait IoStream {
    /// The type of items produced by the stream.
    type Item;
    /// The error type returned by the stream.
    type Error;

    /// Attempts to read the next item from the stream.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(item))` — an item was consumed; the caller is
    ///   pre-woken to drain more.
    /// - `Poll::Ready(None)` — end-of-stream.  This is idempotent and does
    ///   **not** pre-wake; repeated calls keep returning end-of-stream, so
    ///   the caller must recognise EOS and break its read loop.
    /// - `Poll::Pending` — no item is available yet.  The waker will be notified
    ///   when the state changes.
    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Result<Option<Self::Item>, Self::Error>>;

    /// Signals that the reader is no longer interested in further items.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight item is discarded.
    fn drop_read(&self);
}

impl<T, U> IoStream for T
where
    T: RefLockable<Target = U> + ?Sized,
    U: IoStream + ?Sized,
{
    type Item = U::Item;
    type Error = U::Error;

    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let guard = self.lock_ref();
        guard.con_poll_read(cx)
    }

    fn drop_read(&self) {
        let guard = self.lock_ref();
        guard.drop_read()
    }
}

/// A wrapper around a [`Stream`] that implements the [`IoStream`] trait.
///
/// While the inner stream is present, [`con_poll_read`](IoStream::con_poll_read)
/// proxies through to it. Calling [`drop_read`](IoStream::drop_read) drops the
/// inner stream, after which `con_poll_read` returns the EOF signal.
///
/// The inner stream is supplied at construction time and cannot be replaced.
pub struct StreamIoStream<STREAM> {
    inner: RefCell<Option<STREAM>>,
}

impl<STREAM: Stream + Unpin> StreamIoStream<STREAM> {
    /// Creates a new `StreamIoStream` wrapping the given stream.
    pub fn new(inner: STREAM) -> Self {
        Self {
            inner: RefCell::new(Some(inner)),
        }
    }
}

impl<STREAM> core::fmt::Debug for StreamIoStream<STREAM> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StreamIoStream").finish_non_exhaustive()
    }
}

impl<STREAM, ITEM, ERR> IoStream for StreamIoStream<STREAM>
where
    STREAM: Stream<Item = Result<ITEM, ERR>> + Unpin,
{
    type Item = ITEM;
    type Error = ERR;

    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let mut inner = self.inner.borrow_mut();
        if let Some(stream) = inner.as_mut() {
            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    // An item was consumed; pre-wake the caller to drain more.
                    cx.waker().wake_by_ref();
                    Poll::Ready(Ok(Some(item)))
                }
                Poll::Ready(Some(Err(e))) => {
                    // Surface the error and transition to EOF.
                    // Pre-wake the task, since the error is consumed
                    *inner = None; // Transition to EOF state
                    cx.waker().wake_by_ref();
                    Poll::Ready(Err(e))
                }
                Poll::Ready(None) => {
                    // End-of-stream is idempotent, so do not pre-wake.
                    *inner = None; // Transition to EOF state
                    Poll::Ready(Ok(None))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Already in EOF state
            Poll::Ready(Ok(None))
        }
    }

    fn drop_read(&self) {
        *self.inner.borrow_mut() = None; // Transition to EOF state
    }
}
