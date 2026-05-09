//! Connectable wrappers that defer attaching a downstream IO object until runtime.
//!
//! A connectable wrapper starts out in a *disconnected* state: its IO methods
//! return [`Poll::Pending`] and register the caller's waker. Once an inner IO
//! object is provided via `connect`, the registered waker (if any) is woken
//! and all subsequent calls delegate to that inner object.

use core::cell::RefCell;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures::task::AtomicWaker;

use crate::{IoReader, IoSink, IoStream, IoWriter};

enum ConnectableState<T> {
    Disconnected(AtomicWaker),
    Connected(T),
}

impl<T> Drop for ConnectableState<T> {
    fn drop(&mut self) {
        // When a `Disconnected` state is dropped — either because the wrapper
        // is being torn down or because it is being replaced by `Connected` —
        // wake the registered waiter so it re-polls and observes the new state.
        if let ConnectableState::Disconnected(waker) = self {
            waker.wake();
        }
    }
}

/// A connectable [`IoSink`] adapter.
///
/// Starts out disconnected: every `prod_poll_*` call returns [`Poll::Pending`]
/// after registering the caller's waker. When [`connect`](Self::connect) is
/// called with an inner sink, the registered waker (if any) is woken and all
/// subsequent calls delegate to the inner sink.
///
/// [`reset`](Self::reset) returns the wrapper to the disconnected state and
/// drops the inner sink.
pub struct ConnectableIoSink<T> {
    state: RefCell<ConnectableState<T>>,
}

impl<T> Default for ConnectableIoSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoSink<T> {
    /// Creates a new `ConnectableIoSink` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: RefCell::new(ConnectableState::Disconnected(AtomicWaker::new())),
        }
    }

    /// Connects the wrapper to `sink`.
    ///
    /// Replaces any previous state. If the wrapper was disconnected with a
    /// registered waker, that waker is woken so the waiting task re-polls and
    /// observes the now-connected inner sink.
    ///
    /// # Panics
    ///
    /// Panics if called while a `prod_poll_*` method on this wrapper is
    /// currently executing on the same thread.
    pub fn connect(&self, sink: T) {
        *self.state.borrow_mut() = ConnectableState::Connected(sink);
    }

    /// Resets the `ConnectableIoSink` to its initial disconnected state, dropping
    /// the previously connected inner sink, if any.
    ///
    /// If there are any tasks waiting on the inner sink, they will only be woken
    /// if the inner sink's `Drop` impl wakes them. If the wrapper is already in
    /// the disconnected state this is a no-op; in particular any
    /// already-registered waker is preserved.
    ///
    /// # Panics
    ///
    /// Panics if called while a `prod_poll_*` method on this wrapper is
    /// currently executing on the same thread.
    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        if matches!(&*state, ConnectableState::Connected(_)) {
            *state = ConnectableState::Disconnected(AtomicWaker::new());
        }
    }
}

impl<ITEM, T: IoSink<ITEM>> IoSink<ITEM> for ConnectableIoSink<T> {
    type Error = T::Error;

    fn prod_poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.prod_poll_ready(cx),
        }
    }

    fn prod_poll_send(
        &self,
        cx: &mut Context<'_>,
        item: &mut Option<ITEM>,
    ) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.prod_poll_send(cx, item),
        }
    }

    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.prod_poll_flush(cx),
        }
    }

    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.prod_poll_close(cx),
        }
    }
}

/// A connectable [`IoWriter`] adapter.
///
/// Starts out disconnected: every `prod_poll_*` call returns [`Poll::Pending`]
/// after registering the caller's waker. When [`connect`](Self::connect) is
/// called with an inner writer, the registered waker (if any) is woken and
/// all subsequent calls delegate to the inner writer.
///
/// [`reset`](Self::reset) returns the wrapper to the disconnected state and
/// drops the inner writer.
pub struct ConnectableIoWriter<T> {
    state: RefCell<ConnectableState<T>>,
}

impl<T> Default for ConnectableIoWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoWriter<T> {
    /// Creates a new `ConnectableIoWriter` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: RefCell::new(ConnectableState::Disconnected(AtomicWaker::new())),
        }
    }

    /// Connects the wrapper to `writer`.
    ///
    /// Replaces any previous state. If the wrapper was disconnected with a
    /// registered waker, that waker is woken so the waiting task re-polls and
    /// observes the now-connected inner writer.
    ///
    /// # Panics
    ///
    /// Panics if called while a `prod_poll_*` method on this wrapper is
    /// currently executing on the same thread.
    pub fn connect(&self, writer: T) {
        *self.state.borrow_mut() = ConnectableState::Connected(writer);
    }

    /// Resets the `ConnectableIoWriter` to its initial disconnected state,
    /// dropping the previously connected inner writer, if any.
    ///
    /// If there are any tasks waiting on the inner writer, they will only be
    /// woken if the inner writer's `Drop` impl wakes them. If the wrapper is
    /// already disconnected this is a no-op; in particular any
    /// already-registered waker is preserved.
    ///
    /// # Panics
    ///
    /// Panics if called while a `prod_poll_*` method on this wrapper is
    /// currently executing on the same thread.
    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        if matches!(&*state, ConnectableState::Connected(_)) {
            *state = ConnectableState::Disconnected(AtomicWaker::new());
        }
    }
}

impl<T: IoWriter> IoWriter for ConnectableIoWriter<T> {
    type Error = T::Error;

    fn prod_poll_write(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<Result<usize, Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.prod_poll_write(cx, bytes),
        }
    }

    fn prod_poll_flush(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.prod_poll_flush(cx),
        }
    }

    fn prod_poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &*self.state.borrow() {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.prod_poll_close(cx),
        }
    }
}

/// Internal state for the connectable consumer-side wrappers
/// ([`ConnectableIoStream`], [`ConnectableIoReader`]).
///
/// Distinct from [`ConnectableState`] because consumer wrappers must remember
/// a `drop_read` that occurred while disconnected — the producer side may
/// connect later and needs to learn that the consumer has already given up.
enum ConnectableConsumerState<T> {
    Disconnected(AtomicWaker),
    Connected(T),
    Dropped,
}

impl<T> Drop for ConnectableConsumerState<T> {
    fn drop(&mut self) {
        // Wake any task waiting in the disconnected state so that it re-polls
        // and observes the new state (Connected or Closed).
        if let ConnectableConsumerState::Disconnected(waker) = self {
            waker.wake();
        }
    }
}

/// A connectable [`IoStream`] adapter.
///
/// Starts out disconnected: [`con_poll_read`](IoStream::con_poll_read) returns
/// [`Poll::Pending`] after registering the caller's waker. When
/// [`connect`](Self::connect) is called with an inner stream, the registered
/// waker (if any) is woken and all subsequent calls delegate to the inner
/// stream.
///
/// If [`drop_read`](IoStream::drop_read) is called while disconnected, the
/// wrapper transitions to a *closed* state: subsequent
/// [`con_poll_read`](IoStream::con_poll_read) calls return
/// `Poll::Ready(None)` (the EOS signal), and a later [`connect`](Self::connect)
/// will immediately call `drop_read` on the supplied stream and discard it.
pub struct ConnectableIoStream<T> {
    state: RefCell<ConnectableConsumerState<T>>,
}

impl<T> Default for ConnectableIoStream<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoStream<T> {
    /// Creates a new `ConnectableIoStream` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: RefCell::new(ConnectableConsumerState::Disconnected(AtomicWaker::new())),
        }
    }

    /// Resets the `ConnectableIoStream` to its initial disconnected state,
    /// dropping the previously connected inner stream, if any.
    ///
    /// Has no effect when the wrapper is already disconnected.
    ///
    /// # Panics
    ///
    /// Panics if called while a method on this wrapper is currently executing
    /// on the same thread.
    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Disconnected(_) => {
                // No-op; preserve the registered waker.
            }
            _ => {
                *state = ConnectableConsumerState::Disconnected(AtomicWaker::new());
            }
        }
    }
}

impl<T: IoStream> ConnectableIoStream<T> {
    /// Connects the wrapper to `stream`.
    ///
    /// If the wrapper was disconnected with a registered waker, that waker is
    /// woken so the waiting task re-polls and observes the now-connected
    /// inner stream.
    ///
    /// If the wrapper is already in the closed state (because `drop_read` was
    /// called while disconnected), `stream.drop_read()` is invoked and
    /// `stream` is dropped without being stored, so the producer side learns
    /// that the consumer has given up.
    ///
    /// # Panics
    ///
    /// Panics if called while a method on this wrapper is currently executing
    /// on the same thread.
    pub fn connect(&self, stream: T) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Dropped => {
                // Already dropped since the last reset
                stream.drop_read();
            }
            _ => {
                *state = ConnectableConsumerState::Connected(stream);
            }
        }
    }
}

impl<T: IoStream> IoStream for ConnectableIoStream<T> {
    type Item = T::Item;

    fn con_poll_read(&self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &*self.state.borrow() {
            ConnectableConsumerState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableConsumerState::Connected(stream) => stream.con_poll_read(cx),
            ConnectableConsumerState::Dropped => {
                // EOS is treated as consumption; pre-wake per the IoStream contract.
                cx.waker().wake_by_ref();
                Poll::Ready(None)
            }
        }
    }

    fn drop_read(&self) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Connected(stream) => {
                stream.drop_read();
                *state = ConnectableConsumerState::Dropped;
            }
            ConnectableConsumerState::Disconnected(_) => {
                *state = ConnectableConsumerState::Dropped;
            }
            ConnectableConsumerState::Dropped => {}
        }
    }
}

/// A connectable [`IoReader`] adapter.
///
/// Starts out disconnected: [`con_poll_read`](IoReader::con_poll_read) returns
/// [`Poll::Pending`] after registering the caller's waker. When
/// [`connect`](Self::connect) is called with an inner reader, the registered
/// waker (if any) is woken and all subsequent calls delegate to the inner
/// reader.
///
/// If [`drop_read`](IoReader::drop_read) is called while disconnected, the
/// wrapper transitions to a *closed* state: subsequent
/// [`con_poll_read`](IoReader::con_poll_read) calls return
/// `Poll::Ready(Ok(None))` (the EOS signal), and a later
/// [`connect`](Self::connect) will immediately call `drop_read` on the
/// supplied reader and discard it.
pub struct ConnectableIoReader<T> {
    state: RefCell<ConnectableConsumerState<T>>,
}

impl<T> Default for ConnectableIoReader<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoReader<T> {
    /// Creates a new `ConnectableIoReader` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: RefCell::new(ConnectableConsumerState::Disconnected(AtomicWaker::new())),
        }
    }

    /// Resets the `ConnectableIoReader` to its initial disconnected state,
    /// dropping the previously connected inner reader, if any.
    ///
    /// Has no effect when the wrapper is already disconnected.
    ///
    /// # Panics
    ///
    /// Panics if called while a method on this wrapper is currently executing
    /// on the same thread.
    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Disconnected(_) => {
                // No-op; preserve the registered waker.
            }
            _ => {
                *state = ConnectableConsumerState::Disconnected(AtomicWaker::new());
            }
        }
    }
}

impl<T: IoReader> ConnectableIoReader<T> {
    /// Connects the wrapper to `reader`.
    ///
    /// If the wrapper was disconnected with a registered waker, that waker is
    /// woken so the waiting task re-polls and observes the now-connected
    /// inner reader.
    ///
    /// If the wrapper is already in the closed state (because `drop_read` was
    /// called while disconnected), `reader.drop_read()` is invoked and
    /// `reader` is dropped without being stored, so the producer side learns
    /// that the consumer has given up.
    ///
    /// # Panics
    ///
    /// Panics if called while a method on this wrapper is currently executing
    /// on the same thread.
    pub fn connect(&self, reader: T) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Dropped => {
                // Already dropped since the last reset
                reader.drop_read();
            }
            _ => {
                *state = ConnectableConsumerState::Connected(reader);
            }
        }
    }
}

impl<T: IoReader> IoReader for ConnectableIoReader<T> {
    type Error = T::Error;

    fn con_poll_read(
        &self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<Result<Option<Bytes>, Self::Error>> {
        match &*self.state.borrow() {
            ConnectableConsumerState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableConsumerState::Connected(reader) => reader.con_poll_read(cx, max_len),
            ConnectableConsumerState::Dropped => {
                // EOS is treated as consumption (even at max_len == 0); pre-wake
                // per the IoReader contract.
                cx.waker().wake_by_ref();
                Poll::Ready(Ok(None))
            }
        }
    }

    fn drop_read(&self) {
        let mut state = self.state.borrow_mut();
        match &*state {
            ConnectableConsumerState::Connected(reader) => {
                reader.drop_read();
                *state = ConnectableConsumerState::Dropped;
            }
            ConnectableConsumerState::Disconnected(_) => {
                *state = ConnectableConsumerState::Dropped;
            }
            ConnectableConsumerState::Dropped => {}
        }
    }
}
