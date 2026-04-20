//! Watchable value primitives for async change notification, built on the
//! generic intrusive linked list in [`crate::intrusive_list`].
//!
//! This module provides [`WatchableValue`] and [`ValueWatch`], a pair of types
//! that implement efficient, mutex-synchronized value-change notification.
//! A [`WatchableValue`] holds a value of type `T`, and any number of
//! [`ValueWatch`] futures can wait to be notified when the value changes.
//!
//! # Architecture
//!
//! [`WatchNodeValue`] is the [`IntrusiveNodeValue`] implementation that drives
//! the list.  Every `WatchableValue` owns a *head* [`IntrusiveListNode`], and
//! every `ValueWatch` owns a *leaf* node.  The head node's `WatchNodeValue`
//! contains a [`Mutex`] protecting the current value.
//!
//! When the value changes, [`WatchableValue::set`] walks the linked list,
//! and wakes every waiting leaf.  When a `ValueWatch` is polled, it checks
//! to see if it's linked into the list.  If so, then it returns [`Poll::Pending`].
//! If not (which includes first poll), it returns [`Poll::Ready`] with the current
//! value.  Either way it will be linked into the list so it won't return ready again
//! until the value changes.
//!
//! # Safety
//!
//! `WatchableValue` must be pinned before creating any `ValueWatch` against it,
//! and `ValueWatch` must be pinned before polling, because the list stores
//! raw pointers to the nodes' addresses.

use crate::intrusive_list::{IntrusiveListNode, IntrusiveNodeValue};
use parking_lot::Mutex;
use std::{
    cell::UnsafeCell,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

// ---------------------------------------------------------------------------
// WatchNodeValue — IntrusiveNodeValue implementation
// ---------------------------------------------------------------------------

/// The [`IntrusiveNodeValue`] implementation used by [`WatchableValue`] and
/// [`ValueWatch`].
///
/// Head nodes hold a `Mutex<T>` protecting the current value.  Leaf nodes
/// store a [`Waker`] and a raw pointer back to the head node.  When the
/// value changes, all linked leaves are unlinked and woken.  Readiness is
/// determined by link state: an unlinked leaf is ready, a linked leaf is
/// pending.
enum WatchNodeValue<T: Clone> {
    Head {
        mutex: Mutex<T>,
    },
    Node {
        target: *const IntrusiveListNode<WatchNodeValue<T>>,
        waker: UnsafeCell<Option<Waker>>,
    },
}

impl<T: Clone> WatchNodeValue<T> {
    fn new_head(val: T) -> Self {
        WatchNodeValue::Head {
            mutex: Mutex::new(val),
        }
    }

    fn new_node(target: *const IntrusiveListNode<Self>) -> Self {
        WatchNodeValue::Node {
            target,
            waker: UnsafeCell::new(None),
        }
    }

    /// Takes and wakes the stored waker (if any).
    ///
    /// No-op on head nodes.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.
    unsafe fn wake(&self) {
        if let WatchNodeValue::Node { waker, .. } = self {
            if let Some(w) = unsafe { (*waker.get()).take() } {
                w.wake();
            }
        }
    }

    /// Returns a mutable reference to the leaf's stored waker slot.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.  Only valid on leaf nodes.
    unsafe fn set_waker(&self, new_waker: &Waker) {
        match self {
            WatchNodeValue::Node { waker, .. } => unsafe {
                match &mut *waker.get() {
                    None => *waker.get() = Some(new_waker.clone()),
                    Some(old_waker) => {
                        if !new_waker.will_wake(&old_waker) {
                            *waker.get() = Some(new_waker.clone());
                        }
                    }
                }
            },
            WatchNodeValue::Head { .. } => panic!("set_waker called on head node"),
        };
    }
}

impl<T: Clone> IntrusiveNodeValue for WatchNodeValue<T> {
    type HeadValue = T;

    fn lock_mutex(&self) -> parking_lot::MutexGuard<'_, T> {
        match self {
            WatchNodeValue::Head { mutex } => mutex.lock(),
            WatchNodeValue::Node { .. } => panic!("lock_mutex called on leaf node"),
        }
    }

    fn target_node(&self) -> Option<&IntrusiveListNode<Self>> {
        unsafe {
            match self {
                WatchNodeValue::Node { target, .. } => Some(&**target),
                WatchNodeValue::Head { .. } => None,
            }
        }
    }
}

// ===========================================================================
// WatchableValue and ValueWatch
// ===========================================================================

/// A shared value that can wake [`ValueWatch`] futures when it changes.
///
/// The value is protected by a mutex.  When the value is updated via
/// [`set`](Self::set), all linked watchers are unlinked and woken.
///
/// # Pinning
///
/// A `WatchableValue` must be pinned before any [`ValueWatch`] can be created
/// against it, because watchers store raw pointers back to the internal node.
pub struct WatchableValue<T: Clone> {
    head: IntrusiveListNode<WatchNodeValue<T>>,
}

impl<T: Clone> WatchableValue<T> {
    /// Creates a new watchable value with the given initial value.
    pub fn new(val: T) -> Self {
        Self {
            head: IntrusiveListNode::new(WatchNodeValue::new_head(val)),
        }
    }

    /// Sets the value and wakes all watchers.
    ///
    /// Every call to `set` counts as a change, regardless of whether the new
    /// value equals the old.  `T` is therefore not required to implement
    /// [`PartialEq`] — watchers are woken unconditionally, and the next
    /// `watch_poll` will return [`Poll::Ready`].
    pub fn set(&self, val: T) {
        let mut guard = self.head.lock_head();
        *guard = val;
        unsafe {
            guard.filter(|node| {
                node.wake();
                false
            });
        }
    }

    /// Returns a clone of the current value.
    #[inline(always)]
    pub fn get(&self) -> T {
        let guard = self.head.lock_head();
        (*guard).clone()
    }
}

impl<T: Clone + Debug> Debug for WatchableValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.head.lock_head();
        f.debug_struct("WatchableValue")
            .field("value", &*g)
            .finish()
    }
}

/// A future that resolves when a [`WatchableValue`]'s value changes.
///
/// Created via [`ValueWatch::new`] with a reference to a pinned
/// `WatchableValue`.  Implements [`Future`], resolving to `T` (a clone of the
/// new value) when the value has changed since the last poll.
///
/// Both [`Future::poll`] and [`ValueWatch::watch_poll`] follow the `watch_poll*`
/// semantics documented on [`ValueWatch::watch_poll`]: the first poll always
/// returns [`Poll::Ready`]; subsequent polls return [`Poll::Ready`] only if the
/// value has changed since the last poll, and [`Poll::Pending`] otherwise.  The
/// task is always (re-)registered to be woken on the next change, and a
/// [`Poll::Ready`] result consumes the pending change — callers must be
/// prepared to process it on the spot, because they will not be notified of it
/// again.
///
/// After returning [`Poll::Ready`], the watch can be polled again to wait for
/// the next change.
///
/// # Pinning
///
/// The `ValueWatch` must be pinned before polling, because it participates in
/// an intrusive linked list via raw pointers.
///
/// # Lifetime
///
/// The `'a` lifetime ties this watch to its parent value, ensuring the value
/// is not dropped while watches reference it.
pub struct ValueWatch<'a, T: Clone> {
    node: IntrusiveListNode<WatchNodeValue<T>>,
    _lifetime: PhantomData<&'a WatchableValue<T>>,
}

impl<'a, T: Clone> ValueWatch<'a, T> {
    /// Creates a new watch against the given pinned watchable value.
    pub fn new(value: Pin<&'a WatchableValue<T>>) -> Self {
        let head_ref = unsafe { &*Pin::into_inner_unchecked(value) };
        Self {
            node: IntrusiveListNode::new(WatchNodeValue::new_node(
                &head_ref.head as *const IntrusiveListNode<WatchNodeValue<T>>,
            )),
            _lifetime: PhantomData,
        }
    }

    /// Returns the current value and registers the task to be woken on the
    /// next change.
    ///
    /// Behaves like a [`watch_poll`](Self::watch_poll) call that always
    /// returns [`Poll::Ready`]: any pending change is consumed, the current
    /// value is returned, and the task is armed for the next change.  Unlike
    /// `watch_poll`, the caller does not learn whether a change had occurred
    /// since the last poll.
    ///
    /// Useful when the caller wants the current value regardless of whether
    /// it has changed — for example, to seed local state and arm the watch
    /// in a single step.
    pub fn watch_check(self: &Pin<&mut Self>, cx: &mut Context<'_>) -> T {
        let guard = self.node.lock_head();
        unsafe {
            guard.link(&self.node);
            self.node.typ.set_waker(cx.waker());
            (*guard).clone()
        }
    }

    /// Polls the watch without consuming the pin.
    ///
    /// # `watch_poll*` semantics
    ///
    /// A `watch_poll*` method checks (or waits) for changes since the last poll:
    ///
    /// - The first poll on a watch future always returns [`Poll::Ready`].
    /// - Subsequent polls return [`Poll::Ready`] if a change has occurred since the
    ///   last poll, or [`Poll::Pending`] otherwise.
    /// - In ALL cases, the task is registered to be woken on the next change,
    ///   even when the method returns [`Poll::Ready`].
    ///
    /// Calling a `watch_poll*` method *consumes* any changes since the last poll:
    /// the next poll will return [`Poll::Pending`] unless further changes have
    /// occurred.  Callers must therefore be prepared to process the change
    /// whenever [`Poll::Ready`] is returned, because they will not be notified
    /// about it again.
    pub fn watch_poll(self: &Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let guard = self.node.lock_head();
        unsafe {
            let ready = !self.node.is_linked();
            guard.link(&self.node);
            self.node.typ.set_waker(cx.waker());
            match ready {
                true => {
                    // Value has changed since last poll.
                    Poll::Ready((*guard).clone())
                }
                false => Poll::Pending,
            }
        }
    }
}

impl<'a, T: Clone> Future for ValueWatch<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        self.watch_poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct TestWaker {
        wake_count: AtomicUsize,
    }

    impl TestWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                wake_count: AtomicUsize::new(0),
            })
        }

        fn count(&self) -> usize {
            self.wake_count.load(Ordering::SeqCst)
        }
    }

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_watch<T: Clone>(watch: &mut Pin<&mut ValueWatch<'_, T>>, waker: &Waker) -> Poll<T> {
        let mut cx = Context::from_waker(waker);
        watch.as_mut().poll(&mut cx)
    }

    // -----------------------------------------------------------------------
    // Basic tests
    // -----------------------------------------------------------------------

    #[test]
    fn watchable_value_new_and_get() {
        let wv = WatchableValue::new(42u64);
        assert_eq!(wv.get(), 42);
    }

    #[test]
    fn watchable_value_set_and_get() {
        let wv = WatchableValue::new(0u64);
        wv.set(100);
        assert_eq!(wv.get(), 100);
    }

    #[test]
    fn watchable_value_debug() {
        let wv = WatchableValue::new(99u64);
        let dbg = format!("{:?}", wv);
        assert!(dbg.contains("WatchableValue"));
        assert!(dbg.contains("99"));
    }

    // -----------------------------------------------------------------------
    // Polling tests
    // -----------------------------------------------------------------------

    #[test]
    fn first_poll_returns_ready() {
        let wv = pin!(WatchableValue::new(42u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(42u64));
    }

    #[test]
    fn pending_when_value_unchanged() {
        let wv = pin!(WatchableValue::new(42u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(42));

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
        // Poll again without changing the value
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
    }

    #[test]
    fn ready_when_value_changed() {
        let wv = pin!(WatchableValue::new(42u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(42));
        wv.as_ref().get_ref().set(99);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(99));
    }

    #[test]
    fn ready_returns_latest_value() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(0));
        // Multiple changes before next poll
        wv.as_ref().get_ref().set(10);
        wv.as_ref().get_ref().set(20);
        wv.as_ref().get_ref().set(30);
        // Should return the latest value
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(30));
    }

    #[test]
    fn ready_then_ready_then_pending() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        wv.as_ref().get_ref().set(0);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(0));
        assert_eq!(tw.count(), 0);
        // Multiple changes before next poll
        wv.as_ref().get_ref().set(10);
        assert_eq!(tw.count(), 1);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(10));
        assert_eq!(tw.count(), 1);
        wv.as_ref().get_ref().set(20);
        assert_eq!(tw.count(), 2);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(20));
        assert_eq!(tw.count(), 2);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
    }

    // -----------------------------------------------------------------------
    // Wake notification tests
    // -----------------------------------------------------------------------

    #[test]
    fn set_wakes_watcher() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(0));
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
        assert_eq!(tw.count(), 0);

        wv.as_ref().get_ref().set(42);
        assert_eq!(tw.count(), 1);
    }

    #[test]
    fn set_same_value_still_wakes() {
        // Setting the value always counts as a change and wakes watchers,
        // even when the new value equals the old (T: !PartialEq).
        let wv = pin!(WatchableValue::new(42u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(42));
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
        wv.as_ref().get_ref().set(42);
        assert_eq!(tw.count(), 1);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(42));
    }

    // -----------------------------------------------------------------------
    // Multiple watchers
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_watchers_all_notified() {
        let wv = pin!(WatchableValue::new(0u64));
        let w1 = ValueWatch::new(wv.as_ref());
        let w2 = ValueWatch::new(wv.as_ref());
        let mut w1 = pin!(w1);
        let mut w2 = pin!(w2);

        let tw1 = TestWaker::new();
        let tw2 = TestWaker::new();
        let wk1 = Waker::from(tw1.clone());
        let wk2 = Waker::from(tw2.clone());

        assert_eq!(poll_watch(&mut w1, &wk1), Poll::Ready(0));
        assert_eq!(poll_watch(&mut w2, &wk2), Poll::Ready(0));
        assert_eq!(poll_watch(&mut w1, &wk1), Poll::Pending);
        assert_eq!(poll_watch(&mut w2, &wk2), Poll::Pending);

        wv.as_ref().get_ref().set(99);
        assert_eq!(tw1.count(), 1);
        assert_eq!(tw2.count(), 1);
        assert_eq!(poll_watch(&mut w1, &wk1), Poll::Ready(99));
        assert_eq!(poll_watch(&mut w2, &wk2), Poll::Ready(99));
    }

    // -----------------------------------------------------------------------
    // Re-watch after ready
    // -----------------------------------------------------------------------

    #[test]
    fn can_watch_again_after_ready() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // First cycle
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(0));
        wv.as_ref().get_ref().set(10);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(10));

        // Second cycle: pending again until next change
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Pending);
        wv.as_ref().get_ref().set(20);
        assert_eq!(tw.count(), 2);
        assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(20));
    }

    // -----------------------------------------------------------------------
    // Drop safety
    // -----------------------------------------------------------------------

    #[test]
    fn drop_watch_while_linked() {
        let wv = pin!(WatchableValue::new(0u64));
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        {
            let watch = ValueWatch::new(wv.as_ref());
            let mut watch = pin!(watch);
            assert_eq!(poll_watch(&mut watch, &waker), Poll::Ready(0));
            // watch is linked and dropped here
        }

        // Should not panic
        wv.as_ref().get_ref().set(99);
        assert_eq!(tw.count(), 0);
    }

    #[test]
    fn drop_watch_before_polling() {
        let wv = pin!(WatchableValue::new(0u64));
        {
            let _watch = ValueWatch::new(wv.as_ref());
        }
        // Should not panic
        wv.as_ref().get_ref().set(100);
    }

    #[test]
    fn drop_multiple_watches() {
        let wv = pin!(WatchableValue::new(0u64));
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        {
            let w1 = ValueWatch::new(wv.as_ref());
            let mut w1 = pin!(w1);
            {
                let w2 = ValueWatch::new(wv.as_ref());
                let mut w2 = pin!(w2);

                assert_eq!(poll_watch(&mut w1, &waker), Poll::Ready(0));
                assert_eq!(poll_watch(&mut w2, &waker), Poll::Ready(0));
                // w2 drops here
            }
            // w1 should still work
            wv.as_ref().get_ref().set(42);
            assert_eq!(poll_watch(&mut w1, &waker), Poll::Ready(42));
        }
    }

    // -----------------------------------------------------------------------
    // Waker update test
    // -----------------------------------------------------------------------

    #[test]
    fn repoll_updates_waker() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);

        let tw1 = TestWaker::new();
        let wk1 = Waker::from(tw1.clone());
        assert_eq!(poll_watch(&mut watch, &wk1), Poll::Ready(0));
        assert_eq!(poll_watch(&mut watch, &wk1), Poll::Pending);

        let tw2 = TestWaker::new();
        let wk2 = Waker::from(tw2.clone());
        assert_eq!(poll_watch(&mut watch, &wk2), Poll::Pending);

        wv.as_ref().get_ref().set(5);
        assert_eq!(tw1.count(), 0);
        assert_eq!(tw2.count(), 1);
    }

    // -----------------------------------------------------------------------
    // watch_poll test
    // -----------------------------------------------------------------------

    #[test]
    fn watch_poll_returns_ready_then_ready() {
        let wv = pin!(WatchableValue::new(0u64));
        let watch = ValueWatch::new(wv.as_ref());
        let mut watch = pin!(watch);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());
        let mut cx = Context::from_waker(&waker);

        assert_eq!(watch.as_mut().watch_poll(&mut cx), Poll::Ready(0));
        wv.as_ref().get_ref().set(77);
        assert_eq!(watch.as_mut().watch_poll(&mut cx), Poll::Ready(77));
    }

    // -----------------------------------------------------------------------
    // Tokio integration test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tokio_watch_resolves() {
        let wv = pin!(WatchableValue::new(0u64));
        let wv_ref = wv.as_ref();

        let mut watch = pin!(ValueWatch::new(wv_ref));

        let val = watch.as_mut().await;
        assert_eq!(val, 0);

        let addr = wv_ref.get_ref() as *const WatchableValue<u64> as usize;
        let handle = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            unsafe { &*(addr as *const WatchableValue<u64>) }.set(42);
        });

        let val = watch.as_mut().await;
        assert_eq!(val, 42);
        handle.await.unwrap();
    }
}
