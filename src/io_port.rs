//! A lazily-connected, droppable I/O endpoint.
//!
//! [`IoPort`] wraps an optional inner I/O object and mediates access to it from
//! a consumer and a producer task.  It models a connection that may not exist
//! yet (disconnected), may be supplied later via [`IoPort::connect`], or may be
//! permanently abandoned via [`IoPort::try_predrop`].  Once connected it
//! transparently forwards the [`IoStream`], [`IoSink`], [`IoReader`], and
//! [`IoWriter`] traits to the inner value.

use core::cell::Cell;
use core::ops::Deref;
use core::task::Context;
use core::task::Poll;

use bytes::Bytes;
use futures::task::noop_waker_ref;

use crate::{IoReader, IoSink, IoStream, IoWriter, WakerRef};

#[derive(Debug)]
enum PortState<T> {
    Disconnected {
        con_waker: WakerRef,
        prod_waker: WakerRef,
    },
    Connected(T),
}

/// A connection point that holds an optional inner I/O object `T` and brokers
/// access to it from a consumer task and a producer task.
///
/// An `IoPort` starts out *disconnected*.  While disconnected, both
/// [`con_poll_connected`](IoPort::con_poll_connected) and
/// [`prod_poll_connected`](IoPort::prod_poll_connected) return
/// [`Poll::Pending`] and register their respective wakers, until either
/// [`connect`](IoPort::connect) supplies an inner value or the port is
/// *pre-dropped* (see [`try_predrop`](IoPort::try_predrop)), after which they
/// report end-of-connection.  Once connected, the port forwards the
/// [`IoStream`]/[`IoSink`]/[`IoReader`]/[`IoWriter`] traits through to the
/// inner value.
#[derive(Debug)]
pub struct IoPort<T> {
    state: PortState<T>,
    is_predropped: Cell<bool>,
}

impl<T> Drop for IoPort<T> {
    fn drop(&mut self) {
        match &self.state {
            PortState::Disconnected {
                con_waker,
                prod_waker,
            } => {
                con_waker.wake();
                prod_waker.wake();
            }
            PortState::Connected(_) => {}
        }
    }
}

impl<T> IoPort<T> {
    /// Creates a new, disconnected and not-pre-dropped `IoPort`.
    pub fn new() -> Self {
        Self {
            state: PortState::Disconnected {
                con_waker: WakerRef::new(),
                prod_waker: WakerRef::new(),
            },
            is_predropped: Cell::new(false),
        }
    }

    /// Attempts to set the connected value of this port.
    ///
    /// This succeeds if the port is currently disconnected and not pre-dropped,
    /// waking any consumer and producer tasks that were waiting for a
    /// connection.  If the port is pre-dropped or already connected, this has
    /// no effect and returns `Err(())`.
    // The unit error is intentional: failure has only one cause (the port is
    // not in a connectable state) and carries no extra information.
    #[allow(clippy::result_unit_err)]
    pub fn connect(&mut self, value: T) -> Result<(), ()> {
        match &self.state {
            PortState::Disconnected {
                con_waker,
                prod_waker,
            } => {
                if self.is_predropped.get() {
                    Err(())
                } else {
                    con_waker.wake();
                    prod_waker.wake();
                    self.state = PortState::Connected(value);
                    Ok(())
                }
            }
            PortState::Connected(_) => Err(()),
        }
    }

    /// Reset this port to the disconnected, non-predropped state, ready
    /// to be connected again. If this port is currently connected, the connection
    /// will be dropped.  Tasks waiting on the current connection will NOT be woken
    /// unless that is accomplished by dropping the connected value.
    pub fn reset(&mut self) {
        match &self.state {
            PortState::Disconnected { .. } => {}
            PortState::Connected(_) => {
                self.state = PortState::Disconnected {
                    con_waker: WakerRef::new(),
                    prod_waker: WakerRef::new(),
                }
            }
        }
        self.is_predropped.set(false);
    }
    /// Attempts to mark the port as pre-dropped.
    /// If this port is in the disconnected state, this returns `true` and any
    /// subsequent call to `connect` will fail.
    /// Otherwise, this returns false and has no effect.
    pub fn try_predrop(&self) -> bool {
        match &self.state {
            PortState::Disconnected { .. } => {
                self.is_predropped.set(true);
                true
            }
            PortState::Connected(_) => false,
        }
    }

    /// Poll for the connected value as the consumer task.
    /// If this port is currently connected, this returns `Poll::Ready(Some(&T))`.
    /// If this port is currently disconnected and not pre-dropped, this registers
    /// the current task as a consumer waker and returns `Poll::Pending`.
    /// If this port is currently disconnected and pre-dropped, this returns `Poll::Ready(None)`.
    pub fn con_poll_connected(&self, cx: &mut Context<'_>) -> Poll<Option<&T>> {
        match &self.state {
            PortState::Disconnected { con_waker, .. } => {
                if self.is_predropped.get() {
                    Poll::Ready(None)
                } else {
                    con_waker.register(cx.waker());
                    Poll::Pending
                }
            }
            PortState::Connected(connected) => Poll::Ready(Some(connected)),
        }
    }
    /// Poll for the connected value as the producer task.
    /// If this port is currently connected, this returns `Poll::Ready(Some(&T))`.
    /// If this port is currently disconnected and not pre-dropped, this registers
    /// the current task as a producer waker and returns `Poll::Pending`.
    /// If this port is currently disconnected and pre-dropped, this returns `Poll::Ready(None)`.
    pub fn prod_poll_connected(&self, cx: &mut Context<'_>) -> Poll<Option<&T>> {
        match &self.state {
            PortState::Disconnected { prod_waker, .. } => {
                if self.is_predropped.get() {
                    Poll::Ready(None)
                } else {
                    prod_waker.register(cx.waker());
                    Poll::Pending
                }
            }
            PortState::Connected(connected) => Poll::Ready(Some(connected)),
        }
    }
}

impl<T> Default for IoPort<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> IoStream for IoPort<T>
where
    T: Deref<Target = U>,
    U: IoStream + ?Sized,
{
    type Item = U::Item;
    type Error = U::Error;

    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let connected = match self.con_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected,
            Poll::Ready(None) => return Poll::Ready(Ok(None)),
            Poll::Pending => return Poll::Pending,
        };
        connected.con_poll_read(cx)
    }

    fn drop_read(&self) {
        if self.try_predrop() {
            return;
        }
        if let Poll::Ready(Some(connected)) =
            self.con_poll_connected(&mut Context::from_waker(noop_waker_ref()))
        {
            connected.drop_read();
        }
    }
}

impl<T, U, ITEM> IoSink<ITEM> for IoPort<T>
where
    T: Deref<Target = U>,
    U: IoSink<ITEM> + ?Sized,
{
    type Error = U::Error;

    fn prod_poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_ready(cx),
            // The peer is gone for good; nothing left to get ready for.
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn prod_poll_send(
        &self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_send(cx, item),
            // The peer is gone for good; drop the item and report completion.
            Poll::Ready(None) => {
                *item = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_flush(cx),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_close(cx),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, U> IoReader for IoPort<T>
where
    T: Deref<Target = U>,
    U: IoReader + ?Sized,
{
    type Error = U::Error;

    fn con_poll_read(
        &self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<Result<Option<Bytes>, Self::Error>> {
        let connected = match self.con_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected,
            Poll::Ready(None) => return Poll::Ready(Ok(None)),
            Poll::Pending => return Poll::Pending,
        };
        connected.con_poll_read(cx, max_len)
    }

    fn drop_read(&self) {
        if self.try_predrop() {
            return;
        }
        if let Poll::Ready(Some(connected)) =
            self.con_poll_connected(&mut Context::from_waker(noop_waker_ref()))
        {
            connected.drop_read();
        }
    }
}

impl<T, U> IoWriter for IoPort<T>
where
    T: Deref<Target = U>,
    U: IoWriter + ?Sized,
{
    type Error = U::Error;

    fn prod_poll_write(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<Result<usize, Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_write(cx, bytes),
            // The peer is gone for good; report all bytes consumed and discarded.
            Poll::Ready(None) => {
                let n = bytes.len();
                bytes.clear();
                Poll::Ready(Ok(n))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_flush(cx),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.prod_poll_connected(cx) {
            Poll::Ready(Some(connected)) => connected.prod_poll_close(cx),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}
