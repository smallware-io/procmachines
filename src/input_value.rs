//! A single-consumer observable input value.
//!
//! [`InputValue`] holds a value that an external caller writes and a single
//! consumer task reads via a `con_poll_*` method.  Reading registers the
//! consumer's waker, and every write wakes it, so the consumer is notified
//! whenever the value changes.  It is typically used to feed input into a
//! state machine from outside.

use core::task::Context;

use crate::WakerRef;

/// A simple value with that can be observed by a single
/// consumer.
///
/// The consumer uses `con_poll_*` methods that
/// provide access to the value, and also register
/// the consumer task to be awoken whenever the
/// value is modified.
///
/// This struct does *not* have interior mutability, so
/// it is normally used to provide input to a state machine
/// from an external caller.
///
/// The external caller uses the `set` or the `as_mut` methods
/// to set the value for the consumer.
///
/// Note that the `as_mut` method provided by implementing
/// the `AsMut` trait assumes that the caller will modify
/// the value, and will therefore wake the consumer
#[derive(Debug)]
pub struct InputValue<T> {
    consumer: WakerRef,
    value: T,
}

impl<T> InputValue<T> {
    /// Creates a new `InputValue` holding `value`, with no consumer registered.
    pub fn new(value: T) -> Self {
        Self {
            consumer: WakerRef::new(),
            value,
        }
    }

    /**
     * Get a reference to the stored value as the single consumer,
     * and also register the consumer's task to be awoken whenever
     * the value is changed.
     */
    pub fn con_poll_ref(&self, cx: &mut Context<'_>) -> &T {
        self.consumer.register(cx.waker());
        &self.value
    }

    /**
     * Set the stored value and wake the consumer
     */
    pub fn set(&mut self, value: T) {
        self.value = value;
        self.consumer.wake();
    }
}

impl<T> AsRef<T> for InputValue<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T> AsMut<T> for InputValue<T> {
    fn as_mut(&mut self) -> &mut T {
        self.consumer.wake();
        &mut self.value
    }
}
