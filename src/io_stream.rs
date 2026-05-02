//! Interior-mutable stream trait for async item consumption.
//!
//! [`IoStream`] is analogous to [`futures::Stream`], but all methods take
//! `&self` instead of `Pin<&mut Self>`. This makes it usable through shared
//! references, matching the design of [`IoSink`](crate::io_sink::IoSink).
//!
//! The trait is implemented by [`IoExchange`](crate::io_exchange::IoExchange),
//! which provides the reader half of a single-slot rendezvous channel.

use std::{
    cell::RefCell,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

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
///   indicates that something was actually consumed**, the calling task is
///   **immediately pre-woken** so it will be re-polled after processing
///   the output.  This is the opposite of the `prod_poll*` contract on
///   [`IoSink`](crate::io_sink::IoSink): consumers are expected to drain in
///   a loop, so a consuming `Ready` keeps the task scheduled to consume
///   more.
/// - A `Poll::Ready` that does **not** consume anything does not pre-wake,
///   but there are no non-consuming `Ready` return values for `con_poll_read`,
///   so this only applies to other interfaces.
///
/// End-of-stream — [`Poll::Ready(None)`](Poll::Ready) from
/// [`con_poll_read`](IoStream::con_poll_read) — is treated as consumption
/// for the purposes of this contract: each call consumes one repetition of
/// the EOS signal and pre-wakes the caller.  The signal is repeatable, so
/// it is the caller's responsibility to recognise end-of-stream and break
/// out of any loop that would otherwise consume it forever.
pub trait IoStream: Send {
    /// The type of items produced by the stream.
    type Item;

    /// Attempts to read the next item from the stream.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(item))` — an item was consumed; the caller is
    ///   pre-woken to drain more.
    /// - `Poll::Ready(None)` — end-of-stream.  Each call consumes one
    ///   repetition of this repeatable signal and pre-wakes the caller;
    ///   the caller must recognise EOS and break its read loop.
    /// - `Poll::Pending` — no item is available yet.  The waker will be notified
    ///   when the state changes.
    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    /// Signals that the reader is no longer interested in further items.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight item is discarded.
    fn drop_read(&self);
}

/**
 * A wrapper for an internally mutable `Option<T: Stream>` the implements the `IoStream` trait.
 *
 * While the inner stream is `None`, the `IoStream` will be in the EOF condition.
 * Calling `drop_read` will set the inner stream to `None`.
 *
 * `reset` can be called at any time to set the inner value, but note that
 * THIS WILL NOT WAKE ANY WAITING READERS.
 */
pub struct StreamIoStream<STREAM: Stream + Send + Unpin> {
    inner: RefCell<Option<STREAM>>,
}

impl<STREAM: Stream + Send + Unpin> StreamIoStream<STREAM> {
    /// Creates a new `StreamIoStream` wrapping the given stream.
    pub fn new(inner: Option<STREAM>) -> Self {
        Self {
            inner: RefCell::new(inner),
        }
    }

    /// Resets the inner stream to a new value, replacing any existing stream.
    pub fn reset(&self, stream: Option<STREAM>) {
        *self.inner.borrow_mut() = stream;
    }
}

impl<STREAM: Stream + Send + Unpin> IoStream for StreamIoStream<STREAM> {
    type Item = STREAM::Item;

    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.borrow_mut();
        if let Some(stream) = inner.as_mut() {
            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    // An item was consumed; pre-wake the caller to drain more.
                    cx.waker().wake_by_ref();
                    Poll::Ready(Some(item))
                }
                Poll::Ready(None) => {
                    // End-of-stream; consume the signal and pre-wake the caller.
                    *inner = None; // Transition to EOF state
                    cx.waker().wake_by_ref();
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Already in EOF state
            Poll::Ready(None)
        }
    }

    fn drop_read(&self) {
        *self.inner.borrow_mut() = None; // Transition to EOF state
    }
}
