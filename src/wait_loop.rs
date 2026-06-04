//! A polling helper for driving manual `poll`-style loops to completion.
//!
//! [`wait_loop`] wraps a closure that is called with a [`Context`] and returns
//! a [`Loop`] verdict.  This is convenient when a task repeatedly polls several
//! sub-objects (for example the consumer and producer halves of an
//! [`IoExchange`](crate::io_exchange::IoExchange)) and wants to either retry
//! immediately, suspend, or finish, without hand-writing a [`Future`].

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// The verdict returned by a [`wait_loop`] closure on each poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loop<T> {
    /// Work is complete; resolve the future with the given value.
    Done(T),
    /// Made progress — poll the closure again immediately without yielding.
    Again,
    /// No progress is possible right now; return [`Poll::Pending`].  A waker
    /// must already have been registered so the task is woken later.
    Wait,
}

/// Creates a [`Future`] that repeatedly calls `f` until it returns
/// [`Loop::Done`].
///
/// On each poll the closure is invoked with the task's [`Context`]:
/// [`Loop::Again`] re-invokes it immediately, [`Loop::Wait`] yields
/// [`Poll::Pending`], and [`Loop::Done`] resolves the future.
pub fn wait_loop<T, F>(f: F) -> WaitLoop<F>
where
    F: FnMut(&mut Context<'_>) -> Loop<T>,
{
    WaitLoop { f }
}

/// The [`Future`] returned by [`wait_loop`].
pub struct WaitLoop<F> {
    f: F,
}

impl<F: Unpin> Unpin for WaitLoop<F> {}

impl<F> fmt::Debug for WaitLoop<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitLoop").finish()
    }
}

impl<T, F> Future for WaitLoop<F>
where
    F: FnMut(&mut Context<'_>) -> Loop<T>,
{
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // SAFETY: We are not moving out of the pinned field.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match (this.f)(cx) {
                Loop::Done(t) => return Poll::Ready(t),
                Loop::Again => continue,
                Loop::Wait => return Poll::Pending,
            }
        }
    }
}
