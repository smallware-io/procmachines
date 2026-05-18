#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, rc::Rc, sync::Arc};
use core::ops::Deref;
#[cfg(feature = "std")]
use std::{boxed::Box, sync::Arc};

pub trait RefLockable: Send {
    type Target: ?Sized;
    type Guard<'a>: Deref<Target = Self::Target>
    where
        Self: 'a;
    fn lock_ref<'a>(&'a self) -> Self::Guard<'a>;
}

impl<R: lock_api::RawMutex + Send + 'static, T: Send + ?Sized + 'static> RefLockable for lock_api::Mutex<R, T> {
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

impl<T: RefLockable + ?Sized + 'static> RefLockable for Box<T> {
    type Target = T::Target;

    type Guard<'a> = T::Guard<'a>;

    fn lock_ref<'a>(&'a self) -> Self::Guard<'a> {
        self.as_ref().lock_ref()
    }
}
