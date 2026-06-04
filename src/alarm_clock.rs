//! Alarm clock primitives for async timeout management, built on the generic
//! intrusive linked list in [`crate::intrusive_list`].
//!
//! This module provides [`AlarmClock`] and [`ClockAlarm`], a pair of types that
//! implement an efficient, mutex-synchronized alarm system.  An [`AlarmClock`]
//! holds a clock value that is expected to increase monotonically (but can
//! be set back if necessary), and any number of [`ClockAlarm`] futures can
//! register thresholds to be woken when the clock reaches or exceeds their target.
//!
//! # Architecture
//!
//! `ClockNodeValue` is the [`IntrusiveNodeValue`] implementation that drives
//! the list.  Every `AlarmClock` owns a *head* [`IntrusiveListNode`], and every
//! `ClockAlarm` owns a *leaf* node.  The head node's `ClockNodeValue` contains
//! a [`Mutex`] protecting the current clock value; leaf nodes store their alarm
//! threshold and a [`Waker`] in `UnsafeCell`s.
//!
//! When the clock advances, [`AlarmClock::advance`] walks
//! the linked list via [`crate::intrusive_list::IntrusiveListGuard::filter`]
//! and wakes every alarm whose threshold has been met.  When a `ClockAlarm` is
//! polled, it checks the threshold under the lock and links itself into the
//! list if it needs to wait.
//!
//! # Safety
//!
//! `AlarmClock` must be pinned before creating any `ClockAlarm` against it,
//! and `ClockAlarm` must be pinned before polling, because the list stores
//! raw pointers to the nodes' addresses.

use crate::{
    WakerRef,
    intrusive_list::{IntrusiveListNode, IntrusiveNodeValue},
};
use core::{
    cell::UnsafeCell,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use futures::never::Never;
use lock_api::{Mutex, RawMutex};

// ---------------------------------------------------------------------------
// ClockNodeValue — IntrusiveNodeValue implementation for AlarmClock / ClockAlarm
// ---------------------------------------------------------------------------

struct ClockHeadInner<T>
where
    T: PartialOrd + Clone,
{
    // Current time in the clock
    time: T,
    // If this is `None`, then the external waker, if any, needs to be woken when any alarm is set.
    // If this is `Some(t)`, then the external waker needs to be woken only if an alarm is set with
    // deadline < t.
    external_threshold: Option<T>,
    // Waker for the provider task, if it's waiting for an alarm to be set.
    external_waker: WakerRef,
}

impl<T> ClockHeadInner<T>
where
    T: PartialOrd + Clone,
{
    fn new(time: T) -> Self {
        Self {
            time,
            external_threshold: None,
            external_waker: WakerRef::new(),
        }
    }
}

/// The [`IntrusiveNodeValue`] implementation used by [`AlarmClock`] and
/// [`ClockAlarm`].
///
/// Head nodes hold a `Mutex<R,T>` protecting the current clock value.
/// Leaf nodes store an alarm threshold (`Option<T>`), a [`Waker`], and a raw
/// pointer back to the head node.  `None` disables the alarm; comparisons
/// use `PartialOrd`.
enum ClockNodeValue<R: RawMutex, T: PartialOrd + Clone> {
    /// The sentinel head node.  Owns the mutex that protects the clock value
    /// and synchronises all list mutations.
    Head { mutex: Mutex<R, ClockHeadInner<T>> },
    /// A leaf (alarm) node.  Stores the alarm threshold in `val`, a waker to
    /// notify when the alarm fires, and a raw pointer back to the head node
    /// so it can acquire the mutex.
    Node {
        // SAFETY: The `ClockAlarm` constructor ensures that the target (head)
        // is pinned and outlives this node via the `'a` lifetime parameter.
        target: *const IntrusiveListNode<ClockNodeValue<R, T>>,
        val: UnsafeCell<Option<T>>,
        waker: UnsafeCell<Option<Waker>>,
    },
}

impl<R: RawMutex, T: PartialOrd + Clone> ClockNodeValue<R, T> {
    /// Creates a head-node value with the given initial clock value.
    fn new_head(val: T) -> Self {
        ClockNodeValue::Head {
            mutex: Mutex::<R, ClockHeadInner<T>>::new(ClockHeadInner::new(val)),
        }
    }

    /// Creates a leaf-node value targeting the given pinned head node.
    ///
    /// # Safety
    ///
    /// `target` must point to a pinned head node that outlives this leaf.
    fn new_node(target: *const IntrusiveListNode<Self>, val: Option<T>) -> Self {
        ClockNodeValue::Node {
            target,
            val: UnsafeCell::new(val),
            waker: UnsafeCell::new(None),
        }
    }

    /// Returns a reference to the leaf's stored alarm value.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.  Only valid on leaf nodes.
    unsafe fn get_val(&self) -> &Option<T> {
        match self {
            ClockNodeValue::Node { val, .. } => unsafe { &*val.get() },
            ClockNodeValue::Head { .. } => panic!("get_val called on head node"),
        }
    }

    /// Overwrites the leaf's stored alarm value.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.  Only valid on leaf nodes.
    unsafe fn set_val(&self, new_val: Option<T>) {
        match self {
            ClockNodeValue::Node { val, .. } => unsafe { *val.get() = new_val },
            ClockNodeValue::Head { .. } => panic!("set_val called on head node"),
        }
    }

    /// Takes the stored waker (if any) and wakes it.
    ///
    /// No-op on head nodes.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.
    unsafe fn wake(&self) {
        if let ClockNodeValue::Node { waker, .. } = self
            && let Some(w) = unsafe { (*waker.get()).take() }
        {
            w.wake();
        }
    }

    /// Set the node waker.  Only valid on a leaf node
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.  Only valid on leaf nodes.
    unsafe fn set_waker(&self, new_waker: &Waker) {
        match self {
            ClockNodeValue::Node { waker, .. } => unsafe {
                match &mut *waker.get() {
                    None => *waker.get() = Some(new_waker.clone()),
                    Some(old_waker) => {
                        if !old_waker.will_wake(new_waker) {
                            *waker.get() = Some(new_waker.clone());
                        }
                    }
                }
            },
            ClockNodeValue::Head { .. } => panic!("set_waker called on head node"),
        };
    }
    /// Clear the node waker.  Only valid on a leaf node
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.  Only valid on leaf nodes.
    unsafe fn drop_waker(&self) {
        match self {
            ClockNodeValue::Node { waker, .. } => unsafe {
                *waker.get() = None;
            },
            ClockNodeValue::Head { .. } => panic!("set_waker called on head node"),
        };
    }
}

impl<R: RawMutex, T: PartialOrd + Clone> IntrusiveNodeValue for ClockNodeValue<R, T> {
    type HeadValue = ClockHeadInner<T>;
    type RawMutex = R;

    fn lock_list(&self) -> lock_api::MutexGuard<'_, R, ClockHeadInner<T>> {
        match self {
            ClockNodeValue::Head { mutex } => mutex.lock(),
            ClockNodeValue::Node { .. } => panic!("lock_list called on leaf node"),
        }
    }

    fn target_node(&self) -> Option<&IntrusiveListNode<Self>> {
        unsafe {
            match self {
                ClockNodeValue::Node { target, .. } => Some(&**target),
                ClockNodeValue::Head { .. } => None,
            }
        }
    }
}

// ===========================================================================
// AlarmClock and ClockAlarm
//
// Public API: alarms triggered when a monotonically increasing value meets or
// exceeds the alarm threshold.
// ===========================================================================

/// A shared, monotonically-increasing clock that can wake [`ClockAlarm`] futures.
///
/// The clock value is protected by a mutex and can be read or advanced from any
/// thread.  When the value advances past an alarm's threshold, the alarm's waker
/// is invoked, causing the corresponding future to resolve.
///
/// # Pinning
///
/// An `AlarmClock` must be pinned (e.g. via [`pin!`](core::pin::pin) before
/// any [`ClockAlarm`] can be created against it, because alarms store raw pointers
/// back to the clock's internal node.
///
/// # Example
///
/// ```rust,ignore
/// use core::pin::pin;
/// use procmachines::{AlarmClock, ClockAlarm};
///
/// let clock = pin!(AlarmClock::new(0u64));
/// let alarm = pin!(ClockAlarm::new(clock.as_ref(), Some(10)));
///
/// // Later, when time advances:
/// clock.set(10);
/// // `alarm` will resolve on next poll.
/// ```
pub struct AlarmClock<R: RawMutex, T: PartialOrd + Clone> {
    head: IntrusiveListNode<ClockNodeValue<R, T>>,
}

impl<R: RawMutex, T: PartialOrd + Clone> AlarmClock<R, T> {
    /// Creates a new alarm clock with the given initial clock value.
    pub fn new(time: T) -> Self {
        Self {
            head: IntrusiveListNode::new(ClockNodeValue::new_head(time)),
        }
    }

    /// Returns a clone of the current clock value.
    #[inline(always)]
    pub fn get(&self) -> T {
        let guard = self.head.lock_head();
        guard.time.clone()
    }

    /// Advances the clock to `new_time` only if `new_time` is strictly greater than the
    /// current time value.
    ///
    /// If `new_time` is greater than clock's current time value, then the time is advanced
    /// to `new_time`, and any alarms set for times <= `new_time` are woken.
    ///
    /// If the time is advanced and there is a waker registered by `external_poll_new_alarm`, then the
    /// wake threshold is reset to `None`, so if nothing else is done, then it will be woken when the
    /// next alarm is set.  Usually if external polling is being used, then that behaviour should be
    /// reset by a new call to `external_poll_new_alarm` with a new threshold.
    ///
    /// Returns `None` if there are no remaining alarms set.  If there is a remaining alarm set, then
    /// returns `Some(t)` where `t` is the earliest alarm deadline still set after this call.
    pub fn advance(&self, new_time: T) -> Option<T> {
        let mut guard = self.head.lock_head();
        if new_time > guard.time {
            guard.time = new_time;
        }
        guard.external_threshold = None;
        let mut min_alarm: Option<T> = None;
        let val_ref = &(guard.time);
        unsafe {
            guard.filter(|node| match node.get_val() {
                Some(alarm_time) => {
                    if *val_ref >= *alarm_time {
                        node.wake();
                        false
                    } else {
                        if let Some(min_t) = min_alarm.as_ref() {
                            if *alarm_time < *min_t {
                                min_alarm = Some(alarm_time.clone());
                            }
                        } else {
                            min_alarm = Some(alarm_time.clone());
                        }
                        true
                    }
                }
                None => {
                    node.wake();
                    false
                }
            });
        }
        min_alarm
    }

    /// Register an interest in a new alarm being set..
    ///
    /// If wake_threshold is None, then an interest is registered in any alarm being set.  Otherwise, an interest
    /// is registered in any alarm being set with an alarm time < `wake_threshold`.
    ///
    /// If a new alarm is subsequently set that meets the interest, then the provider's waker is awoken.
    ///
    /// Note that the provider's waker is NOT automatically awoken if there are existing alarms that meet the interest.
    /// Normally, `advance` would be called before this method to determine if there are any alarms that require immediate
    /// action.
    ///
    /// Returns Poll::Pending ALWAYS
    pub fn external_poll_new_alarm(
        &self,
        cx: &mut Context<'_>,
        wake_threshold: Option<T>,
    ) -> Poll<Never> {
        let mut guard = self.head.lock_head();
        guard.external_threshold = wake_threshold;
        guard.external_waker.register(cx.waker());
        Poll::Pending
    }
}

impl<R: RawMutex, T: PartialOrd + Clone + Debug> Debug for AlarmClock<R, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let g = self.head.lock_head();
        f.debug_struct("AlarmClock")
            .field("val", &(g.time))
            .finish()
    }
}

/// A future that resolves when an [`AlarmClock`]'s value reaches a threshold.
///
/// Created via [`ClockAlarm::new`] with a reference to a pinned `AlarmClock`.
/// The alarm threshold can be changed at any time via
/// [`set_alarm`](Self::set_alarm), and setting it to `None` disables the alarm
/// (the future will pend indefinitely).
///
/// # `alarm_poll` semantics
///
/// [`alarm_poll`](Self::alarm_poll) (and the [`Future`] impl, which delegates
/// to it) monitors an edge-triggered, idempotent event: the clock time first
/// meeting or exceeding the alarm threshold.
///
/// All `alarm_poll*` methods in this crate have "alarm polling" semantics:
///
/// - If the condition is **not yet met**, the call returns [`Poll::Pending`]
///   and the task's waker is registered.  The waker will fire when the clock
///   advances past the threshold.
/// - If the condition **is currently met**, the call returns
///   [`Poll::Ready(())`].  Further calls continue to return `Ready` (the event
///   is idempotent) until the alarm is reset via [`set_alarm`](Self::set_alarm)
///   or the clock itself is set back to an earlier value.  The task is not
///   registered to be woken.
/// - [`set_alarm`](Self::set_alarm) resets the alarm threshold but **never**
///   registers or wakes the task — only `alarm_poll` methods do that. If the
///   task was previously registered to wake, that registration is dropped.
///
/// # Pinning
///
/// The `ClockAlarm` must be pinned before polling, because it participates in
/// an intrusive linked list via raw pointers.
///
/// # Lifetime
///
/// The `'a` lifetime ties this alarm to its parent clock, ensuring the clock
/// is not dropped while alarms reference it.
pub struct ClockAlarm<'a, R: RawMutex, T: PartialOrd + Clone> {
    node: IntrusiveListNode<ClockNodeValue<R, T>>,
    _lifetime: PhantomData<&'a AlarmClock<R, T>>,
}

impl<R: RawMutex, T: PartialOrd + Clone> Debug for ClockAlarm<'_, R, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ClockAlarm").finish_non_exhaustive()
    }
}

impl<'a, R: RawMutex, T: PartialOrd + Clone> ClockAlarm<'a, R, T> {
    /// Creates a new alarm against the given pinned clock.
    ///
    /// `wake_at` is the threshold value; the alarm fires when the clock reaches
    /// or exceeds it.  Pass `None` to create a disabled alarm that can be armed
    /// later via [`set_alarm`](Self::set_alarm).
    pub fn new(clock: Pin<&'a AlarmClock<R, T>>, wake_at: Option<T>) -> Self {
        let head_ref = unsafe { Pin::into_inner_unchecked(clock) };
        Self {
            node: IntrusiveListNode::new(ClockNodeValue::new_node(
                &head_ref.head as *const IntrusiveListNode<ClockNodeValue<R, T>>,
                wake_at,
            )),
            _lifetime: PhantomData,
        }
    }

    /// Changes the alarm threshold.
    ///
    /// Any previous registration (waker + list link) is unconditionally
    /// dropped.  The task will not be woken by a subsequent clock advance
    /// unless [`alarm_poll`](Self::alarm_poll) is called again to
    /// re-register.
    ///
    /// This method **never** registers or wakes the task — only
    /// `alarm_poll` methods do that.
    pub fn set_alarm(&self, wake_at: Option<T>) {
        let guard = self.node.lock_head();
        unsafe {
            guard.unlink(&self.node);
            self.node.typ.drop_waker();
            self.node.typ.set_val(wake_at);
        }
    }

    /// Returns the current alarm threshold, or `None` if disabled.
    pub fn get_alarm(&self) -> Option<&T> {
        let _guard = self.node.lock_head();
        unsafe { self.node.typ.get_val().as_ref() }
    }

    /// Polls the alarm without consuming the pin.
    ///
    /// This is the primary polling method for `ClockAlarm`.  It implements
    /// **alarm_poll** semantics: it waits for an edge-triggered, idempotent
    /// event — the clock time first meeting or exceeding the alarm threshold.
    ///
    /// - Returns [`Poll::Pending`] if the clock has not yet reached the
    ///   threshold.  The task's waker is registered and will fire when the
    ///   clock advances past the threshold.
    /// - Returns [`Poll::Ready(())`] if the threshold is currently met.
    ///   Further calls continue to return `Ready` until the alarm is reset
    ///   via [`set_alarm`](Self::set_alarm) or the clock is set back to an
    ///   earlier value.
    /// - If the alarm is disabled (`None` threshold), always returns
    ///   `Pending`.
    ///
    /// This method is useful when you need to poll from a `select!` or
    /// similar combinator that provides `&Pin<&mut Self>` rather than
    /// consuming the `Pin<&mut Self>`.
    pub fn alarm_poll(self: &Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<()> {
        let mut guard = self.node.lock_head();
        unsafe {
            match self.node.typ.get_val() {
                None => {
                    // Alarm disabled — unlink and pend forever.
                    guard.unlink(&self.node);
                    return Poll::Pending;
                }
                Some(alarm) => {
                    if guard.time >= *alarm {
                        // Threshold met — unlink without registering a waker.
                        // Any previous waker was already consumed by the clock
                        // advance that triggered this re-poll, or was never
                        // stored (first poll with condition already met).
                        guard.unlink(&self.node);
                        return Poll::Ready(());
                    } else if let Some(threshold) = guard.external_threshold.as_ref() {
                        if *alarm < *threshold {
                            guard.external_threshold = None;
                            guard.external_waker.wake();
                        }
                    } else {
                        guard.external_waker.wake();
                    }
                }
            }

            // Clock hasn't reached our threshold yet.
            // Store / update the waker so a future clock change can notify us.
            self.node.typ.set_waker(cx.waker());
            // Link into the list so a future head update can wake us.
            guard.link(&self.node);
        }
        Poll::Pending
    }
}

impl<'a, R: RawMutex, T: PartialOrd + Clone> Future for ClockAlarm<'a, R, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<Self::Output> {
        self.alarm_poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RawMutex;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    // -----------------------------------------------------------------------
    // Test waker that counts wake() calls
    // -----------------------------------------------------------------------
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

    /// Helper: poll a ClockAlarm and return the Poll result.
    fn poll_alarm<T: PartialOrd + Clone>(
        alarm: &mut Pin<&mut ClockAlarm<'_, RawMutex, T>>,
        waker: &Waker,
    ) -> Poll<()> {
        let mut cx = Context::from_waker(waker);
        alarm.as_mut().poll(&mut cx)
    }

    /// Helper: call AlarmClock::set through a Pin<&mut AlarmClock> without
    /// accidentally hitting Pin::set (which expects a whole AlarmClock value).
    fn clock_set(clock: &Pin<&mut AlarmClock<RawMutex, u64>>, val: u64) {
        clock.as_ref().get_ref().advance(val);
    }

    /// Helper: call ClockAlarm::set_alarm through a Pin<&mut ClockAlarm>.
    fn alarm_set(alarm: &Pin<&mut ClockAlarm<'_, RawMutex, u64>>, val: Option<u64>) {
        alarm.as_ref().get_ref().set_alarm(val);
    }

    type TestClock = AlarmClock<RawMutex, u64>;

    /// Helper: register external interest via `external_poll_new_alarm`,
    /// asserting (as the method guarantees) that it always returns `Pending`.
    fn ext_poll(clock: &Pin<&mut TestClock>, waker: &Waker, threshold: Option<u64>) {
        let mut cx = Context::from_waker(waker);
        assert!(
            clock
                .as_ref()
                .get_ref()
                .external_poll_new_alarm(&mut cx, threshold)
                .is_pending()
        );
    }

    /// White-box helper: read the head's current `external_threshold`.
    fn ext_threshold(clock: &Pin<&mut TestClock>) -> Option<u64> {
        clock.as_ref().get_ref().head.lock_head().external_threshold
    }

    // -----------------------------------------------------------------------
    // AlarmClock basic tests
    // -----------------------------------------------------------------------

    #[test]
    fn alarm_clock_new_and_get() {
        let clock = TestClock::new(42u64);
        assert_eq!(clock.get(), 42);
    }

    #[test]
    fn alarm_clock_set() {
        let clock = TestClock::new(0u64);
        clock.advance(100);
        assert_eq!(clock.get(), 100);
        // can't advance backward
        clock.advance(50);
        assert_eq!(clock.get(), 100);
    }

    #[test]
    fn alarm_clock_advance_only_forward() {
        let clock = TestClock::new(10u64);
        // Advance to a larger value succeeds
        assert_eq!(clock.advance(20), None);
        assert_eq!(clock.get(), 20);
        // Advance to same value fails
        assert_eq!(clock.advance(20), None);
        assert_eq!(clock.get(), 20);
        // Advance to smaller value fails
        assert_eq!(clock.advance(20), None);
        assert_eq!(clock.get(), 20);
    }

    #[test]
    fn alarm_clock_debug() {
        let clock = TestClock::new(99u64);
        let dbg = format!("{:?}", clock);
        assert!(dbg.contains("AlarmClock"));
        assert!(dbg.contains("99"));
    }

    // -----------------------------------------------------------------------
    // ClockAlarm creation and get/set
    // -----------------------------------------------------------------------

    #[test]
    fn clock_alarm_new_and_get() {
        let clock = pin!(TestClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        assert_eq!(alarm.get_alarm(), Some(&10));
    }

    #[test]
    fn clock_alarm_new_none_threshold() {
        let clock = pin!(TestClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), None);
        assert_eq!(alarm.get_alarm(), None);
    }

    #[test]
    fn clock_alarm_set_threshold() {
        let clock = pin!(TestClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        alarm.set_alarm(Some(20));
        assert_eq!(alarm.get_alarm(), Some(&20));
        alarm.set_alarm(None);
        assert_eq!(alarm.get_alarm(), None);
    }

    // -----------------------------------------------------------------------
    // Polling tests
    // -----------------------------------------------------------------------

    #[test]
    fn poll_pending_when_clock_below_threshold() {
        let clock = pin!(TestClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    #[test]
    fn poll_ready_when_clock_at_threshold() {
        let clock = pin!(TestClock::new(10u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    #[test]
    fn poll_ready_when_clock_above_threshold() {
        let clock = pin!(AlarmClock::new(20u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    #[test]
    fn poll_pending_when_threshold_is_none() {
        let clock = pin!(AlarmClock::new(100u64));
        let alarm = ClockAlarm::new(clock.as_ref(), None);
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Even with a large clock value, None threshold means Never → Pending
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    // -----------------------------------------------------------------------
    // Wake notification tests
    // -----------------------------------------------------------------------

    #[test]
    fn advance_clock_wakes_alarm() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // First poll: pending, registers waker
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
        assert_eq!(tw.count(), 0);

        // Advance clock past threshold
        clock_set(&clock, 10);

        // Waker should have been called
        assert_eq!(tw.count(), 1);

        // Next poll: ready
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    #[test]
    fn advance_clock_below_threshold_does_not_wake() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Advance, but not enough
        clock_set(&clock, 5);
        assert_eq!(tw.count(), 0);

        // Still pending
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    #[test]
    fn set_alarm_to_already_passed_value_does_not_wake() {
        let clock = pin!(AlarmClock::new(50u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(100));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Poll to register the waker
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
        assert_eq!(tw.count(), 0);

        // Change the alarm threshold to something already passed.
        // set_alarm never wakes — registration is dropped.
        alarm_set(&alarm, Some(30));
        assert_eq!(tw.count(), 0);

        // But next poll sees clock(50) >= alarm(30) → Ready
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    #[test]
    fn set_alarm_to_none_does_not_wake() {
        let clock = pin!(AlarmClock::new(50u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(100));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Disable the alarm
        alarm_set(&alarm, None);
        // Never → no wake
        assert_eq!(tw.count(), 0);

        // Still pending (disabled)
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    // -----------------------------------------------------------------------
    // Multiple alarms
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_alarms_different_thresholds() {
        let clock = pin!(AlarmClock::new(0u64));

        let a1 = ClockAlarm::new(clock.as_ref(), Some(5));
        let a2 = ClockAlarm::new(clock.as_ref(), Some(10));
        let a3 = ClockAlarm::new(clock.as_ref(), Some(15));
        let mut a1 = pin!(a1);
        let mut a2 = pin!(a2);
        let mut a3 = pin!(a3);

        let tw1 = TestWaker::new();
        let tw2 = TestWaker::new();
        let tw3 = TestWaker::new();
        let w1 = Waker::from(tw1.clone());
        let w2 = Waker::from(tw2.clone());
        let w3 = Waker::from(tw3.clone());

        // All pending
        assert_eq!(poll_alarm(&mut a1, &w1), Poll::Pending);
        assert_eq!(poll_alarm(&mut a2, &w2), Poll::Pending);
        assert_eq!(poll_alarm(&mut a3, &w3), Poll::Pending);

        // Advance to 5: only a1 fires
        clock_set(&clock, 5);
        assert_eq!(tw1.count(), 1);
        assert_eq!(tw2.count(), 0);
        assert_eq!(tw3.count(), 0);
        assert_eq!(poll_alarm(&mut a1, &w1), Poll::Ready(()));
        assert_eq!(poll_alarm(&mut a2, &w2), Poll::Pending);
        assert_eq!(poll_alarm(&mut a3, &w3), Poll::Pending);

        // Advance to 12: a2 fires, a3 still pending
        clock_set(&clock, 12);
        assert_eq!(tw2.count(), 1);
        assert_eq!(tw3.count(), 0);
        assert_eq!(poll_alarm(&mut a2, &w2), Poll::Ready(()));
        assert_eq!(poll_alarm(&mut a3, &w3), Poll::Pending);

        // Advance to 20: a3 fires
        clock_set(&clock, 20);
        assert_eq!(tw3.count(), 1);
        assert_eq!(poll_alarm(&mut a3, &w3), Poll::Ready(()));
    }

    #[test]
    fn multiple_alarms_same_threshold() {
        let clock = pin!(AlarmClock::new(0u64));
        let a1 = ClockAlarm::new(clock.as_ref(), Some(10));
        let a2 = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut a1 = pin!(a1);
        let mut a2 = pin!(a2);

        let tw1 = TestWaker::new();
        let tw2 = TestWaker::new();
        let w1 = Waker::from(tw1.clone());
        let w2 = Waker::from(tw2.clone());

        assert_eq!(poll_alarm(&mut a1, &w1), Poll::Pending);
        assert_eq!(poll_alarm(&mut a2, &w2), Poll::Pending);

        clock_set(&clock, 10);
        assert_eq!(tw1.count(), 1);
        assert_eq!(tw2.count(), 1);
        assert_eq!(poll_alarm(&mut a1, &w1), Poll::Ready(()));
        assert_eq!(poll_alarm(&mut a2, &w2), Poll::Ready(()));
    }

    // -----------------------------------------------------------------------
    // Re-arm after firing
    // -----------------------------------------------------------------------

    #[test]
    fn alarm_can_be_rearmed_after_firing() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(5));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
        clock_set(&clock, 5);
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));

        // Re-arm with a new threshold
        alarm_set(&alarm, Some(20));
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        clock_set(&clock, 20);
        assert_eq!(tw.count(), 2); // woken twice total
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    // -----------------------------------------------------------------------
    // Drop safety
    // -----------------------------------------------------------------------

    #[test]
    fn drop_alarm_while_linked() {
        let clock = pin!(AlarmClock::new(0u64));
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        {
            let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
            let mut alarm = pin!(alarm);
            assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
            // alarm is linked and dropped here
        }

        // Clock advance should not panic (the dropped alarm was properly unlinked)
        clock_set(&clock, 10);
        assert_eq!(tw.count(), 0); // waker was taken during drop/unlink
    }

    #[test]
    fn drop_alarm_before_polling() {
        let clock = pin!(AlarmClock::new(0u64));
        {
            let _alarm = ClockAlarm::new(clock.as_ref(), Some(10));
            // Never polled, just dropped
        }
        // Should not panic
        clock_set(&clock, 100);
    }

    #[test]
    fn drop_multiple_alarms() {
        let clock = pin!(AlarmClock::new(0u64));
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        {
            let a1 = ClockAlarm::new(clock.as_ref(), Some(10));
            let mut a1 = pin!(a1);
            {
                let a2 = ClockAlarm::new(clock.as_ref(), Some(20));
                let mut a2 = pin!(a2);

                assert_eq!(poll_alarm(&mut a1, &waker), Poll::Pending);
                assert_eq!(poll_alarm(&mut a2, &waker), Poll::Pending);
                // a2 drops here while both are linked
            }
            // a1 should still work
            clock_set(&clock, 10);
            assert_eq!(poll_alarm(&mut a1, &waker), Poll::Ready(()));
        }
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn poll_ready_immediately_no_waker_stored() {
        // If the clock already meets the threshold on first poll, the alarm
        // should return Ready without storing a waker.
        let clock = pin!(AlarmClock::new(100u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(50));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
        // No spurious wakes
        assert_eq!(tw.count(), 0);
    }

    #[test]
    fn repoll_pending_alarm_updates_waker() {
        // Polling twice with different wakers should update the stored waker.
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);

        let tw1 = TestWaker::new();
        let w1 = Waker::from(tw1.clone());
        assert_eq!(poll_alarm(&mut alarm, &w1), Poll::Pending);

        let tw2 = TestWaker::new();
        let w2 = Waker::from(tw2.clone());
        assert_eq!(poll_alarm(&mut alarm, &w2), Poll::Pending);

        // Advance: should wake tw2, not tw1
        clock_set(&clock, 10);
        assert_eq!(tw1.count(), 0);
        assert_eq!(tw2.count(), 1);
    }

    #[test]
    fn advance_does_not_go_backwards() {
        let clock = pin!(AlarmClock::new(10u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(5));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Already past threshold
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));

        // Re-arm alarm at 15
        alarm_set(&alarm, Some(15));
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Try to advance backwards — should fail
        assert_eq!(clock.advance(5), Some(15));
        assert_eq!(clock.get(), 10);
        assert_eq!(tw.count(), 0);

        // Advance forward
        assert_eq!(clock.advance(15), None);
        assert_eq!(tw.count(), 1);
    }

    #[test]
    fn set_clock_backwards_wakes_appropriate_alarms() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Set clock to 20, alarm at 10 should fire
        clock_set(&clock, 20);
        assert_eq!(tw.count(), 1);
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));

        // Re-arm at 30
        alarm_set(&alarm, Some(30));
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Set clock backwards to 5 — alarm at 30 should NOT fire
        clock_set(&clock, 5);
        assert_eq!(tw.count(), 1); // unchanged
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    // -----------------------------------------------------------------------
    // Tokio integration test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tokio_alarm_resolves() {
        let clock = pin!(AlarmClock::new(0u64));
        let clock_ref = clock.as_ref();

        let mut alarm = pin!(ClockAlarm::new(clock_ref, Some(10)));

        // Convert to usize to make it Send; SAFETY: clock outlives the spawned task
        // and AlarmClock is Sync (via IntrusiveListNode's unsafe impl).
        let addr = clock_ref.get_ref() as *const TestClock as usize;
        let handle = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            unsafe { &*(addr as *const TestClock) }.advance(10);
        });

        alarm.as_mut().await;
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn tokio_multiple_alarms_resolve_in_order() {
        let clock = pin!(AlarmClock::new(0u64));
        let clock_ref = clock.as_ref();

        let mut a1 = pin!(ClockAlarm::new(clock_ref, Some(5)));
        let mut a2 = pin!(ClockAlarm::new(clock_ref, Some(10)));

        let addr = clock_ref.get_ref() as *const TestClock as usize;
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(30));
            let clock = unsafe { &*(addr as *const TestClock) };
            clock.advance(5);
            std::thread::sleep(std::time::Duration::from_millis(30));
            clock.advance(10);
        });

        a1.as_mut().await;
        a2.as_mut().await;
    }

    // -----------------------------------------------------------------------
    // alarm_poll semantics tests
    // -----------------------------------------------------------------------

    #[test]
    fn ready_is_idempotent() {
        let clock = pin!(AlarmClock::new(10u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(5));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Condition is met from the start — Ready every time, no wakes.
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
        assert_eq!(tw.count(), 0);
    }

    #[test]
    fn set_alarm_drops_registration() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Register the waker
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // set_alarm drops the registration — even though the new threshold
        // is still not met, the waker is cleared and the node is unlinked.
        alarm_set(&alarm, Some(20));

        // Advance clock past BOTH old and new thresholds.
        // The task must NOT be woken because set_alarm dropped the registration.
        clock_set(&clock, 25);
        assert_eq!(tw.count(), 0);

        // But alarm_poll sees clock(25) >= alarm(20) → Ready
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Ready(()));
    }

    #[test]
    fn set_alarm_none_drops_registration() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());

        // Register the waker
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);

        // Disable the alarm — drops registration
        alarm_set(&alarm, None);

        // Advance clock — task must not be woken
        clock_set(&clock, 100);
        assert_eq!(tw.count(), 0);

        // alarm_poll with None threshold → Pending forever
        assert_eq!(poll_alarm(&mut alarm, &waker), Poll::Pending);
    }

    #[test]
    fn alarm_poll_returns_pending_then_ready() {
        let clock = pin!(AlarmClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let waker = Waker::from(tw.clone());
        let mut cx = Context::from_waker(&waker);

        assert_eq!(alarm.as_mut().alarm_poll(&mut cx), Poll::Pending);
        clock_set(&clock, 10);
        assert_eq!(alarm.as_mut().alarm_poll(&mut cx), Poll::Ready(()));
    }

    // -----------------------------------------------------------------------
    // external_poll_new_alarm tests
    //
    // `external_poll_new_alarm` registers a "provider" waker that should be
    // notified when a *new* alarm is registered (via `alarm_poll`) that is
    // earlier than the provider's current interest threshold.  The notification
    // itself is delivered from `alarm_poll`'s pending branch; this method only
    // records the threshold + waker and always returns `Pending`.
    // -----------------------------------------------------------------------

    #[test]
    fn external_poll_always_pending() {
        // The method is infallible-pending: `Poll<Never>` can only ever be
        // `Pending`, but assert it explicitly for both threshold shapes.
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let w = Waker::from(ext.clone());
        ext_poll(&clock, &w, None);
        ext_poll(&clock, &w, Some(5));
    }

    #[test]
    fn external_poll_records_threshold() {
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let w = Waker::from(ext.clone());

        // Fresh clock starts with no recorded threshold.
        assert_eq!(ext_threshold(&clock), None);

        ext_poll(&clock, &w, Some(5));
        assert_eq!(ext_threshold(&clock), Some(5));

        // A subsequent registration overwrites the threshold.
        ext_poll(&clock, &w, None);
        assert_eq!(ext_threshold(&clock), None);

        ext_poll(&clock, &w, Some(99));
        assert_eq!(ext_threshold(&clock), Some(99));
    }

    #[test]
    fn external_woken_on_any_alarm_when_threshold_none() {
        // threshold == None means "wake me on ANY new alarm".
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, None);

        let alarm = ClockAlarm::new(clock.as_ref(), Some(1000));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);
        // With a `None` threshold the recorded threshold stays `None`.
        assert_eq!(ext_threshold(&clock), None);
    }

    #[test]
    fn external_woken_on_earlier_alarm_resets_threshold() {
        // threshold == Some(50) means "wake me when an alarm < 50 is set".
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, Some(50));

        let alarm = ClockAlarm::new(clock.as_ref(), Some(30));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);
        // After firing, the threshold is reset to `None` so the provider is
        // expected to re-register on its next poll.
        assert_eq!(ext_threshold(&clock), None);
    }

    #[test]
    fn external_not_woken_on_equal_or_later_alarm() {
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, Some(50));

        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        // An alarm exactly at the threshold (50) is NOT earlier → no wake.
        let a_eq = ClockAlarm::new(clock.as_ref(), Some(50));
        let mut a_eq = pin!(a_eq);
        assert_eq!(poll_alarm(&mut a_eq, &w), Poll::Pending);
        assert_eq!(ext.count(), 0);
        assert_eq!(ext_threshold(&clock), Some(50)); // untouched

        // A later alarm (60) is also not earlier → no wake.
        let a_late = ClockAlarm::new(clock.as_ref(), Some(60));
        let mut a_late = pin!(a_late);
        assert_eq!(poll_alarm(&mut a_late, &w), Poll::Pending);
        assert_eq!(ext.count(), 0);
        assert_eq!(ext_threshold(&clock), Some(50)); // still untouched
    }

    #[test]
    fn external_not_woken_by_ready_alarm() {
        // An alarm whose threshold is already met resolves immediately and is
        // never "registered" — the provider has nothing to schedule, so it
        // must not be woken.
        let clock = pin!(TestClock::new(100u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, None);

        let alarm = ClockAlarm::new(clock.as_ref(), Some(50));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Ready(()));
        assert_eq!(ext.count(), 0);
        // Threshold left as-is (the ready path returns before touching it).
        assert_eq!(ext_threshold(&clock), None);
    }

    #[test]
    fn external_not_woken_by_disabled_alarm() {
        // A disabled (`None`) alarm has no deadline, so the provider must not
        // be woken when it is polled.
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, None);

        let alarm = ClockAlarm::new(clock.as_ref(), None);
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext.count(), 0);
    }

    #[test]
    fn external_reregistration_updates_waker() {
        // Registering a second provider waker replaces the first.
        let clock = pin!(TestClock::new(0u64));
        let ext1 = TestWaker::new();
        let ext2 = TestWaker::new();
        let ew1 = Waker::from(ext1.clone());
        let ew2 = Waker::from(ext2.clone());

        ext_poll(&clock, &ew1, None);
        ext_poll(&clock, &ew2, None); // replaces ew1

        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext1.count(), 0);
        assert_eq!(ext2.count(), 1);
    }

    #[test]
    fn external_not_woken_without_registration() {
        // No `external_poll_new_alarm` call → no provider waker → polling an
        // alarm must not panic and obviously cannot wake anything.
        let clock = pin!(TestClock::new(0u64));
        let alarm = ClockAlarm::new(clock.as_ref(), Some(10));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext_threshold(&clock), None);
    }

    #[test]
    fn external_only_first_earlier_alarm_wakes() {
        // Once an earlier alarm fires the provider, the threshold resets to
        // `None`, but the waker slot has been consumed — so a second earlier
        // alarm does not double-wake the (now absent) provider until it
        // re-registers.
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, Some(50));

        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        let a1 = ClockAlarm::new(clock.as_ref(), Some(40));
        let mut a1 = pin!(a1);
        assert_eq!(poll_alarm(&mut a1, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);

        // Provider has not re-registered; another earlier alarm finds an empty
        // waker slot (threshold is now `None`, so it takes the "any" path).
        let a2 = ClockAlarm::new(clock.as_ref(), Some(20));
        let mut a2 = pin!(a2);
        assert_eq!(poll_alarm(&mut a2, &w), Poll::Pending);
        assert_eq!(ext.count(), 1); // not woken again
    }

    #[test]
    fn advance_resets_external_threshold_without_waking() {
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());
        ext_poll(&clock, &ew, Some(50));
        assert_eq!(ext_threshold(&clock), Some(50));

        // Advancing the clock resets the interest threshold to `None` but does
        // NOT itself wake the provider.
        clock.as_ref().get_ref().advance(10);
        assert_eq!(ext_threshold(&clock), None);
        assert_eq!(ext.count(), 0);

        // Because the threshold is now `None` (and the provider waker is still
        // registered — advance does not consume it), the very next alarm of any
        // deadline wakes the provider.
        let alarm = ClockAlarm::new(clock.as_ref(), Some(100));
        let mut alarm = pin!(alarm);
        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());
        assert_eq!(poll_alarm(&mut alarm, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);
    }

    #[test]
    fn external_provider_full_cycle() {
        // End-to-end shape of how a timer provider uses the external API.
        let clock = pin!(TestClock::new(0u64));
        let ext = TestWaker::new();
        let ew = Waker::from(ext.clone());

        let tw = TestWaker::new();
        let w = Waker::from(tw.clone());

        // No alarms yet: provider asks to be woken on ANY alarm.
        ext_poll(&clock, &ew, None);

        // A far alarm (100) appears → provider notified.
        let far = ClockAlarm::new(clock.as_ref(), Some(100));
        let mut far = pin!(far);
        assert_eq!(poll_alarm(&mut far, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);

        // Provider reschedules for the earliest deadline (100): only wake me if
        // something strictly earlier than 100 shows up.
        ext_poll(&clock, &ew, Some(100));

        // A later alarm (200) must not disturb the provider.
        let later = ClockAlarm::new(clock.as_ref(), Some(200));
        let mut later = pin!(later);
        assert_eq!(poll_alarm(&mut later, &w), Poll::Pending);
        assert_eq!(ext.count(), 1);

        // An earlier alarm (50) must wake the provider and reset the threshold.
        let earlier = ClockAlarm::new(clock.as_ref(), Some(50));
        let mut earlier = pin!(earlier);
        assert_eq!(poll_alarm(&mut earlier, &w), Poll::Pending);
        assert_eq!(ext.count(), 2);
        assert_eq!(ext_threshold(&clock), None);
    }
}
