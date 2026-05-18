#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, rc::Rc, sync::Arc};
use core::ops::DerefMut;
#[cfg(feature = "std")]
use std::{boxed::Box, rc::Rc, sync::Arc};

pub trait MutLockable {
    type Target: ?Sized;
    type Guard<'a>: DerefMut<Target = Self::Target>
    where
        Self: 'a;
    fn lock_mut<'a>(&'a self) -> Self::Guard<'a>;
}

impl<R: lock_api::RawMutex + 'static, T: ?Sized + 'static> MutLockable for lock_api::Mutex<R, T> {
    type Target = T;
    type Guard<'a> = lock_api::MutexGuard<'a, R, T>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.lock()
    }
}

#[cfg(feature = "std")]
impl<T: ?Sized + 'static> MutLockable for std::sync::Mutex<T> {
    type Target = T;
    type Guard<'a> = std::sync::MutexGuard<'a, T>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.lock().unwrap()
    }
}

impl<T: MutLockable + ?Sized + 'static> MutLockable for Arc<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_mut()
    }
}

impl<T: MutLockable + ?Sized + 'static> MutLockable for Rc<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_mut()
    }
}

impl<T: MutLockable + ?Sized + 'static> MutLockable for Box<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_mut<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_mut()
    }
}
