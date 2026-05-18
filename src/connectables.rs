//! Connectable wrappers that defer attaching a downstream IO object until runtime.
//!
//! A connectable wrapper starts out in a *disconnected* state: its IO methods
//! return [`Poll::Pending`] and register the caller's waker. Once an inner IO
//! object is provided via `connect`, the registered waker (if any) is woken
//! and all subsequent calls delegate to that inner object.

use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use crate::waker_ref::WakerRef;
use bytes::Bytes;

use crate::mut_lockable::MutLockable;
use crate::{IoReader, IoSink, IoStream, IoWriter};

enum ConnectableState<T> {
    Disconnected(WakerRef),
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
pub struct ConnectableIoSink<T: MutLockable + Send> {
    state: ConnectableState<T>,
}

impl<T> Default for ConnectableIoSink<T>
where
    T: MutLockable + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoSink<T>
where
    T: MutLockable + Send,
{
    /// Creates a new `ConnectableIoSink` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: ConnectableState::Disconnected(WakerRef::new()),
        }
    }

    /// Connects the wrapper to `sink`.
    ///
    /// Replaces any previous state. If the wrapper was disconnected with a
    /// registered waker, that waker is woken so the waiting task re-polls and
    /// observes the now-connected inner sink.
    pub fn connect(&mut self, sink: T) {
        self.state = ConnectableState::Connected(sink);
    }

    /// Resets the `ConnectableIoSink` to its initial disconnected state, dropping
    /// the previously connected inner sink, if any.
    ///
    /// If there are any tasks waiting on the inner sink, they will only be woken
    /// if the inner sink's `Drop` impl wakes them. If the wrapper is already in
    /// the disconnected state this is a no-op; in particular any
    /// already-registered waker is preserved.
    pub fn reset(&mut self) {
        if matches!(&self.state, ConnectableState::Connected(_)) {
            self.state = ConnectableState::Disconnected(WakerRef::new());
        }
    }
}

impl<T, I, U> IoSink<I> for ConnectableIoSink<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoSink<I> + ?Sized,
{
    type Error = U::Error;

    fn prod_poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.lock_mut().prod_poll_ready(cx),
        }
    }

    fn prod_poll_send(
        &mut self,
        cx: &mut Context<'_>,
        item: &mut Option<I>,
    ) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.lock_mut().prod_poll_send(cx, item),
        }
    }

    fn prod_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.lock_mut().prod_poll_flush(cx),
        }
    }

    fn prod_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(sink) => sink.lock_mut().prod_poll_close(cx),
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
pub struct ConnectableIoWriter<T: MutLockable + Send> {
    state: ConnectableState<T>,
}

impl<T> Default for ConnectableIoWriter<T>
where
    T: MutLockable + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoWriter<T>
where
    T: MutLockable + Send,
{
    /// Creates a new `ConnectableIoWriter` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: ConnectableState::Disconnected(WakerRef::new()),
        }
    }

    /// Connects the wrapper to `writer`.
    ///
    /// Replaces any previous state. If the wrapper was disconnected with a
    /// registered waker, that waker is woken so the waiting task re-polls and
    /// observes the now-connected inner writer.
    pub fn connect(&mut self, writer: T) {
        self.state = ConnectableState::Connected(writer);
    }

    /// Resets the `ConnectableIoWriter` to its initial disconnected state,
    /// dropping the previously connected inner writer, if any.
    ///
    /// If there are any tasks waiting on the inner writer, they will only be
    /// woken if the inner writer's `Drop` impl wakes them. If the wrapper is
    /// already disconnected this is a no-op; in particular any
    /// already-registered waker is preserved.
    pub fn reset(&mut self) {
        if matches!(&self.state, ConnectableState::Connected(_)) {
            self.state = ConnectableState::Disconnected(WakerRef::new());
        }
    }
}

impl<T, U> IoWriter for ConnectableIoWriter<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoWriter + ?Sized,
{
    type Error = U::Error;

    fn prod_poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<Result<usize, Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.lock_mut().prod_poll_write(cx, bytes),
        }
    }

    fn prod_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.lock_mut().prod_poll_flush(cx),
        }
    }

    fn prod_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(writer) => writer.lock_mut().prod_poll_close(cx),
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
/// If [`drop_read`](IoStream::drop_read) is called, the wrapper transitions to
/// a *closed* state: subsequent [`con_poll_read`](IoStream::con_poll_read)
/// calls return `Poll::Ready(None)` (the EOS signal), and a later
/// [`connect`](Self::connect) will immediately call `drop_read` on the
/// supplied stream and discard it.
pub struct ConnectableIoStream<T: MutLockable + Send> {
    state: ConnectableState<T>,
    drop_pending: AtomicBool,
}

impl<T> Default for ConnectableIoStream<T>
where
    T: MutLockable + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoStream<T>
where
    T: MutLockable + Send,
{
    /// Creates a new `ConnectableIoStream` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: ConnectableState::Disconnected(WakerRef::new()),
            drop_pending: AtomicBool::new(false),
        }
    }

    /// Resets the `ConnectableIoStream` to its initial disconnected state,
    /// dropping the previously connected inner stream, if any.
    ///
    /// Has no effect when the wrapper is already in the unconsumed disconnected
    /// state.
    pub fn reset(&mut self) {
        *self.drop_pending.get_mut() = false;
        if matches!(&self.state, ConnectableState::Connected(_)) {
            self.state = ConnectableState::Disconnected(WakerRef::new());
        }
    }
}

impl<T, U> ConnectableIoStream<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoStream + ?Sized,
{
    /// Connects the wrapper to `stream`.
    ///
    /// If the wrapper was disconnected with a registered waker, that waker is
    /// woken so the waiting task re-polls and observes the now-connected
    /// inner stream.
    ///
    /// If the wrapper is already in the closed state (because `drop_read` was
    /// called previously), `stream.drop_read()` is invoked and `stream` is
    /// dropped without being stored, so the producer side learns that the
    /// consumer has given up.
    pub fn connect(&mut self, stream: T) {
        if *self.drop_pending.get_mut() {
            stream.lock_mut().drop_read();
            *self.drop_pending.get_mut() = false;
        }
        self.state = ConnectableState::Connected(stream);
    }
}

impl<T, U> IoStream for ConnectableIoStream<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoStream + ?Sized,
{
    type Item = U::Item;
    type Error = U::Error;

    fn con_poll_read(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                if self.drop_pending.load(Ordering::Acquire) {
                    // EOS is terminal and repeatable; no pre-wake.
                    return Poll::Ready(Ok(None));
                }
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(stream) => stream.lock_mut().con_poll_read(cx),
        }
    }

    fn drop_read(&mut self) {
        match &mut self.state {
            ConnectableState::Connected(stream) => stream.lock_mut().drop_read(),
            ConnectableState::Disconnected(waker) => {
                if !self.drop_pending.load(Ordering::Relaxed) {
                    self.drop_pending.store(true, Ordering::Release);
                    waker.wake();
                }
            }
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
/// If [`drop_read`](IoReader::drop_read) is called, the wrapper transitions to
/// a *closed* state: subsequent [`con_poll_read`](IoReader::con_poll_read)
/// calls return `Poll::Ready(Ok(None))` (the EOS signal), and a later
/// [`connect`](Self::connect) will immediately call `drop_read` on the
/// supplied reader and discard it.
pub struct ConnectableIoReader<T: MutLockable + Send> {
    state: ConnectableState<T>,
    drop_pending: AtomicBool,
}

impl<T> Default for ConnectableIoReader<T>
where
    T: MutLockable + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectableIoReader<T>
where
    T: MutLockable + Send,
{
    /// Creates a new `ConnectableIoReader` in the disconnected state.
    pub fn new() -> Self {
        Self {
            state: ConnectableState::Disconnected(WakerRef::new()),
            drop_pending: AtomicBool::new(false),
        }
    }

    /// Resets the `ConnectableIoReader` to its initial disconnected state,
    /// dropping the previously connected inner reader, if any.
    ///
    /// Has no effect when the wrapper is already in the unconsumed disconnected
    /// state.
    pub fn reset(&mut self) {
        *self.drop_pending.get_mut() = false;
        if matches!(&self.state, ConnectableState::Connected(_)) {
            self.state = ConnectableState::Disconnected(WakerRef::new());
        }
    }
}

impl<T, U> ConnectableIoReader<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoReader + ?Sized,
{
    /// Connects the wrapper to `reader`.
    ///
    /// If the wrapper was disconnected with a registered waker, that waker is
    /// woken so the waiting task re-polls and observes the now-connected
    /// inner reader.
    ///
    /// If the wrapper is already in the closed state (because `drop_read` was
    /// called previously), `reader.drop_read()` is invoked and `reader` is
    /// dropped without being stored, so the producer side learns that the
    /// consumer has given up.
    pub fn connect(&mut self, reader: T) {
        if *self.drop_pending.get_mut() {
            reader.lock_mut().drop_read();
        } else {
            self.state = ConnectableState::Connected(reader);
        }
        *self.drop_pending.get_mut() = false;
    }
}

impl<T, U> IoReader for ConnectableIoReader<T>
where
    T: MutLockable<Target = U> + Send,
    U: IoReader + ?Sized,
{
    type Error = U::Error;

    fn con_poll_read(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<Result<Option<Bytes>, Self::Error>> {
        match &mut self.state {
            ConnectableState::Disconnected(waker) => {
                if self.drop_pending.load(Ordering::Acquire) {
                    // EOS is terminal and repeatable; no pre-wake.
                    return Poll::Ready(Ok(None));
                }
                waker.register(cx.waker());
                Poll::Pending
            }
            ConnectableState::Connected(reader) => reader.lock_mut().con_poll_read(cx, max_len),
        }
    }

    fn drop_read(&mut self) {
        match &mut self.state {
            ConnectableState::Connected(reader) => reader.lock_mut().drop_read(),
            ConnectableState::Disconnected(waker) => {
                if !self.drop_pending.load(Ordering::Relaxed) {
                    self.drop_pending.store(true, Ordering::Release);
                    waker.wake();
                }
            }
        }
    }
}
