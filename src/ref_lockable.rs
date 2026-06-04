//! Abstractions over shared-reference and exclusive locking.
//!
//! [`RefLockable`] and [`MutLockable`] generalise "give me access to the
//! wrapped value behind a guard" over the various interior-mutability and
//! locking primitives this crate works with — [`RefCell`], [`lock_api::Mutex`],
//! [`std::sync::Mutex`], and [`Arc`] wrappers thereof.  They let generic code
//! (for example the blanket trait impls in [`crate::io_stream`] and
//! [`crate::io_sink`]) accept "anything that can hand out a `&Target`" without
//! caring whether the synchronisation is single-threaded or mutex-based.

#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
use core::cell::RefCell;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::Arc;

/// A container that can hand out shared (`&Target`) access to its contents
/// through a guard.
///
/// This is implemented for [`RefCell`], [`lock_api::Mutex`],
/// [`std::sync::Mutex`] (with the `std` feature), and any [`Arc`] wrapping a
/// `Sync` implementor, allowing generic code to obtain a reference to the
/// wrapped value regardless of the underlying synchronisation strategy.
pub trait RefLockable: Send {
    /// The wrapped value made accessible through the guard.
    type Target: ?Sized;
    /// The guard type that dereferences to [`Target`](Self::Target).
    type Guard<'a>: Deref<Target = Self::Target>
    where
        Self: 'a;
    /// Acquires shared access to the wrapped value, blocking if necessary.
    fn lock_ref<'a>(&'a self) -> Self::Guard<'a>;
}

impl<T: Send + ?Sized + 'static> RefLockable for RefCell<T> {
    type Target = T;
    type Guard<'a> = core::cell::Ref<'a, T>;

    fn lock_ref<'a>(&'a self) -> Self::Guard<'a> {
        self.borrow()
    }
}

impl<R: lock_api::RawMutex + Send + 'static, T: Send + ?Sized + 'static> RefLockable
    for lock_api::Mutex<R, T>
{
    type Target = T;
    type Guard<'a> = lock_api::MutexGuard<'a, R, T>;

    fn lock_ref<'a>(&'a self) -> Self::Guard<'a> {
        self.lock()
    }
}

#[cfg(feature = "std")]
impl<T: Send + ?Sized + 'static> RefLockable for std::sync::Mutex<T> {
    type Target = T;
    type Guard<'a> = std::sync::MutexGuard<'a, T>;

    fn lock_ref<'a>(&'a self) -> Self::Guard<'a> {
        self.lock().unwrap()
    }
}

impl<T: RefLockable + Sync + ?Sized + 'static> RefLockable for Arc<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_ref<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_ref()
    }
}

/// A container that can hand out exclusive (`&mut Target`) access to its
/// contents through a guard.
///
/// This is the mutable counterpart to [`RefLockable`], implemented for
/// [`RefCell`], [`lock_api::Mutex`], [`std::sync::Mutex`] (with the `std`
/// feature), and any [`Arc`] wrapping a `Sync` implementor.
pub trait MutLockable: Send {
    /// The wrapped value made accessible through the guard.
    type Target: ?Sized;
    /// The guard type that mutably dereferences to [`Target`](Self::Target).
    type Guard<'a>: DerefMut<Target = Self::Target>
    where
        Self: 'a;
    /// Acquires exclusive access to the wrapped value, blocking if necessary.
    fn lock_mut<'a>(&'a self) -> Self::Guard<'a>;
}

impl<T: Send + ?Sized + 'static> MutLockable for RefCell<T> {
    type Target = T;
    type Guard<'a> = core::cell::RefMut<'a, T>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.borrow_mut()
    }
}

impl<T: MutLockable + Sync + ?Sized + 'static> MutLockable for Arc<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_mut()
    }
}

impl<R: lock_api::RawMutex + Send + 'static, T: Send + ?Sized + 'static> MutLockable
    for lock_api::Mutex<R, T>
{
    type Target = T;
    type Guard<'a> = lock_api::MutexGuard<'a, R, T>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.lock()
    }
}

#[cfg(feature = "std")]
impl<T: Send + ?Sized + 'static> MutLockable for std::sync::Mutex<T> {
    type Target = T;
    type Guard<'a> = std::sync::MutexGuard<'a, T>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.lock().unwrap()
    }
}
