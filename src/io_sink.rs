//! Single-producer sink trait for async item delivery.
//!
//! [`IoSink`] is analogous to [`futures::Sink`], but all methods take
//! `&mut self` instead of `Pin<&mut Self>`, so implementations do not need
//! to be pinned.
//!
//! The trait is implemented by [`IoExchange`](crate::io_exchange::IoExchange),
//! which provides a single-slot rendezvous channel between a writer and reader

use core::{
    cell::RefCell,
    task::{Context, Poll},
};

use futures::{Sink, SinkExt};

/// A single-producer `Sink`-like trait with `&mut self` receivers.
///
/// Implementations are expected to be `Send` so the sink can be moved
/// between tasks.
///
/// # Protocol
///
/// The expected calling pattern mirrors [`futures::Sink`]:
///
/// 1. [`prod_poll_ready`](IoSink::prod_poll_ready) — wait for capacity.
/// 2. [`prod_poll_send`](IoSink::prod_poll_send) — submit an item.
/// 3. Optionally [`prod_poll_flush`](IoSink::prod_poll_flush) — ensure delivery.
/// 4. [`prod_poll_close`](IoSink::prod_poll_close) — signal end-of-stream.
///
/// # `prod_poll*` semantics
///
/// The `prod_poll*` methods on this trait are intended for use by a single
/// "producer" — a task that is producing items (or progress toward delivery)
/// to be consumed downstream.
///
/// The unifying rule: **whenever a `prod_poll*` return indicates that
/// transmission of the requested information to the receiver is not yet
/// complete, the calling task is arranged to wake when more progress can
/// be made.**
///
/// - If it returns [`Poll::Pending`], the calling task is registered to
///   be woken later when the same call might return [`Poll::Ready`] —
///   the standard `poll` contract.
/// - If it returns [`Poll::Ready`] indicating **complete** transmission
///   (e.g. the item was accepted, or a flush or close handshake was
///   acknowledged), the calling task is **not** pre-woken.  A `Ready`
///   return is simply a signal that the producer may proceed.
/// - If it returns [`Poll::Ready`] indicating only **partial** progress,
///   the calling task is arranged to wake when more progress can be
///   made.  Implementations should prefer registering the waker on the
///   underlying blocking condition; if that is not feasible, they pre-wake
///   the caller so the producer re-polls and either makes more progress
///   or observes the blocking condition itself.  None of the sink
///   methods defined here currently have partial-completion returns —
///   items are atomic — but the rule applies to any future extensions
///   that gain them.
///
/// Producers are expected to produce everything they can without blocking.
/// Consequently, if no `prod_poll*` method ever returns [`Poll::Pending`],
/// the producer should eventually end up blocked on an input poll (such as
/// a `watch_poll*` or other upstream source) — unless it has finished its
/// task entirely.
pub trait IoSink<ITEM> {
    /// The error type returned by sink operations.
    type Error;

    /// Checks whether the sink is ready to accept an item.
    ///
    /// Returns `Poll::Ready(Ok(()))` when a subsequent [`prod_poll_send`](IoSink::prod_poll_send)
    /// is expected to succeed immediately, assuming no other caller intervenes.
    /// Returns `Poll::Pending` when the sink is full or busy.
    fn prod_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Attempts to send an item into the sink.
    ///
    /// If the item is successfully consumed, `*item` is set to `None` and
    /// `Poll::Ready(Ok(()))` is returned. If the sink is not ready, `*item`
    /// is left unchanged and `Poll::Pending` is returned.
    ///
    /// If `item` is `None` on entry, returns `Poll::Ready(Ok(()))` immediately
    /// (no-op send).
    fn prod_poll_send(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), Self::Error>>;

    /// Requests that any buffered items be delivered, and checks progress.
    ///
    /// The definition of "flushed" is implementation-specific. For
    /// [`IoExchange`](crate::io_exchange::IoExchange), flushing completes when
    /// the reader has consumed the in-flight item and then performed a read
    /// that observes the empty slot (confirming it has seen all sent data).
    ///
    /// Returns `Poll::Ready(Ok(()))` once the flush is acknowledged.
    fn prod_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Signals that no further items will be sent and waits for the close
    /// handshake to complete.
    ///
    /// After closing, the reader will eventually observe end-of-stream. If an
    /// item is still in flight, the close is deferred until the reader consumes
    /// it.
    ///
    /// Returns `Poll::Ready(Ok(()))` once the close is fully acknowledged.
    fn prod_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}

/// A wrapper around a [`Sink`] that implements the [`IoSink`] trait.
///
/// All methods proxy to the inner sink's [`Sink`] implementation. The inner
/// sink is supplied at construction time and cannot be replaced.
pub struct SinkIoSink<ITEM, SINK: Sink<ITEM>> {
    inner: RefCell<SINK>,
    _phantom: core::marker::PhantomData<fn(ITEM)>,
}

impl<ITEM, SINK> SinkIoSink<ITEM, SINK>
where
    SINK: Sink<ITEM> + Unpin,
{
    pub fn new(inner: SINK) -> Self {
        Self {
            inner: RefCell::new(inner),
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<ITEM, SINK> IoSink<ITEM> for SinkIoSink<ITEM, SINK>
where
    SINK: Sink<ITEM> + Unpin,
{
    type Error = SINK::Error;

    fn prod_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.borrow_mut().poll_ready_unpin(cx)
    }

    fn prod_poll_send(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), Self::Error>> {
        if item.is_none() {
            return Poll::Ready(Ok(()));
        }
        let mut inner = self.inner.borrow_mut();
        match inner.poll_ready_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(_)) => (),
        };
        match inner.start_send_unpin(item.take().unwrap()) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn prod_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.borrow_mut().poll_flush_unpin(cx)
    }

    fn prod_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.borrow_mut().poll_close_unpin(cx)
    }
}
