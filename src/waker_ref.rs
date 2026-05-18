//! Non-atomic, single-threaded analogue of [`futures::task::AtomicWaker`].
//!
//! [`WakerRef`] holds at most one [`Waker`] for later notification, with the
//! same `register` / `wake` / `take` interface as `AtomicWaker`, but uses
//! plain interior mutability ([`Cell`]) instead of atomics.  The struct is
//! `Send` but **not** `Sync` — it may be moved between threads, but it
//! must not be accessed from multiple threads concurrently.

use core::cell::Cell;
use core::task::Waker;

/// A single-waker slot with non-atomic interior mutability.
///
/// Use this where the surrounding type already enforces exclusive access
/// (for example, behind a `&mut self` API or a lock) and the atomic
/// machinery of [`futures::task::AtomicWaker`] would be wasted.
#[derive(Default)]
pub struct WakerRef {
    waker: Cell<Option<Waker>>,
}

impl core::fmt::Debug for WakerRef {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Peek at the slot without disturbing it.
        let registered = self.waker.take();
        let has = registered.is_some();
        self.waker.set(registered);
        f.debug_struct("WakerRef")
            .field("registered", &has)
            .finish()
    }
}

impl WakerRef {
    /// Creates a new, empty slot.
    pub const fn new() -> Self {
        Self {
            waker: Cell::new(None),
        }
    }

    /// Records `waker` as the task to notify on the next [`wake`](Self::wake)
    /// call, replacing any previously registered waker unless the new one
    /// already wakes the same task.
    pub fn register(&self, waker: &Waker) {
        match self.waker.take() {
            Some(prev) if prev.will_wake(waker) => {
                self.waker.set(Some(prev));
            }
            _ => {
                self.waker.set(Some(waker.clone()));
            }
        }
    }

    /// Wakes the registered waker, if any, and clears the slot.
    pub fn wake(&self) {
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    /// Removes and returns the registered waker, if any, without waking it.
    pub fn take(&self) -> Option<Waker> {
        self.waker.take()
    }
}
