//! Single-consumer stream trait for async item consumption.
//!
//! [`IoStream`] is analogous to [`futures::Stream`], but all methods take
//! `&mut self` instead of `Pin<&mut Self>`, matching the design of
//! [`IoSink`](crate::io_sink::IoSink).
//!
//! The trait is implemented by [`IoExchange`](crate::io_exchange::IoExchange),
//! which provides the reader half of a single-slot rendezvous channel.

use core::{
    cell::RefCell,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

/// A `Stream`-like trait with `&mut self` receivers.
///
/// Paired with [`IoSink`](crate::io_sink::IoSink) to form a full duplex
/// channel.
///
/// # `con_poll*` semantics
///
/// The `con_poll*` methods on this trait are intended for use by a single
/// "consumer" — a task that is consuming items (or end-of-stream signals)
/// that have been produced upstream.
///
/// The unifying rule: **the calling task is registered to be woken
/// whenever calling the same method again might return a different
/// result.**
///
/// - If a `con_poll*` method returns [`Poll::Pending`], the calling task
///   is registered to be woken later when the same call might return
///   [`Poll::Ready`] — the standard `poll` contract.
/// - If a `con_poll*` method returns [`Poll::Ready`] **and consumes an
///   item**, the calling task is **pre-woken**, because the next call
///   cannot return the same item.  Consumers are expected to drain in a
///   loop, so a consuming `Ready` keeps the task scheduled.
/// - If a `con_poll*` method returns a terminal [`Poll::Ready`] —
///   end-of-stream ([`Ready(Ok(None))`](Poll::Ready)) or a persistent
///   error that the next call would re-emit — the caller is **not**
///   pre-woken: the same call would just return the same value again.
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
    /// - `Poll::Ready(None)` — end-of-stream.  The signal is repeatable;
    ///   subsequent calls return the same value, so the caller is **not**
    ///   pre-woken.  The caller must recognise EOS and break its read
    ///   loop.
    /// - `Poll::Pending` — no item is available yet.  The waker will be notified
    ///   when the state changes.
    fn con_poll_read(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>>;

    /// Signals that the reader is no longer interested in further items.
    ///
    /// After calling this, the writer side will observe that the reader has
    /// been dropped and will receive appropriate errors on subsequent sends.
    /// Any in-flight item is discarded.
    fn drop_read(&mut self);
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

impl<STREAM, ITEM, ERR> IoStream for StreamIoStream<STREAM>
where
    STREAM: Stream<Item = Result<ITEM, ERR>> + Unpin,
{
    type Item = ITEM;
    type Error = ERR;

    fn con_poll_read(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let mut inner = self.inner.borrow_mut();
        if let Some(stream) = inner.as_mut() {
            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    // An item was consumed; pre-wake the caller to drain more.
                    cx.waker().wake_by_ref();
                    Poll::Ready(Ok(Some(item)))
                }
                Poll::Ready(Some(Err(e))) => {
                    // Terminal: subsequent calls return EOF, but this
                    // particular error is one-shot, so pre-wake.
                    *inner = None;
                    cx.waker().wake_by_ref();
                    Poll::Ready(Err(e))
                }
                Poll::Ready(None) => {
                    // End-of-stream — terminal and repeatable; no pre-wake.
                    *inner = None;
                    Poll::Ready(Ok(None))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Already in EOF state
            Poll::Ready(Ok(None))
        }
    }

    fn drop_read(&mut self) {
        *self.inner.borrow_mut() = None; // Transition to EOF state
    }
}
