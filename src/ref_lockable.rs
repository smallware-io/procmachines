#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
use core::cell::RefCell;
use core::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::Arc;

pub trait RefLockable: Send {
    type Target: ?Sized;
    type Guard<'a>: Deref<Target = Self::Target>
    where
        Self: 'a;
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

pub trait MutLockable: Send {
    type Target: ?Sized;
    type Guard<'a>: DerefMut<Target = Self::Target>
    where
        Self: 'a;
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
