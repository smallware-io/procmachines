#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]
//! Procedural state machines built on `async`/`await`.
//!
//! It's often advantageous to encapsulate logic in a *state machine*: a simple
//! object with all of its inputs and outputs on its surface, disconnected from
//! any I/O or infrastructure so that it is easy to test and reuse.  State
//! machines are pleasant to *use* but painful to *write* by hand, because every
//! input has to return control to the caller, which rules out ordinary
//! procedural control flow.
//!
//! Rust's `async`/`await` already compiles procedural functions into state
//! machines.  `procmachines` lets you take advantage of that transformation:
//! you write your logic as one or more `async` functions over a shared *IO*
//! struct, and the crate drives them as a synchronously-advancing state machine
//! that needs no async runtime.
//!
//! # Features
//!
//! - Implement state machines as `async` functions.
//! - Sans-IO support, so logic can be tested with no real I/O.
//! - Connect to real I/O instead of writing data-shuttling tasks.
//! - No async runtime required.
//! - `no_std` support (one [`Arc`](alloc::sync::Arc) is required for
//!   initialization).
//!
//! # Overview
//!
//! - A [`ProcMachine`] owns an IO struct and a set of `async` tasks.  Build one
//!   with [`PROC_MACHINE_BUILDER`] and access its IO through
//!   [`MutLockable::lock_mut`] / [`RefLockable::lock_ref`].
//! - Sans-IO building blocks: the [`IoStream`], [`IoSink`], [`IoReader`], and
//!   [`IoWriter`] traits, with [`IoExchange`] and [`IoBytesExchange`] as
//!   single-slot rendezvous channels, [`IoPort`] for dynamically connecting
//!   real I/O, and [`InputValue`] / [`WatchableValue`] for observable values.
//! - Time handling via [`AlarmClock`] and [`ClockAlarm`].
//! - The [`wait_loop()`] helper for driving manual `poll`-style loops.
//!
//! See the [`examples`] directory for complete, runnable programs, and the
//! crate README for a narrative walkthrough.
//!
//! # Polling roles and semantics
//!
//! This crate contains many pollable structs and traits.  They are polled much
//! like a [`Future`], but with some extensions: some objects are polled by more
//! than one task, and most can be polled many times and return `Ready` many
//! times.  When a method returns `Pending` the implications match a `Future`
//! returning `Pending`; when it returns `Ready` it will often still register
//! the caller's waker to be notified the next time the information it consumed
//! changes.
//!
//! Every polling method has a prefix indicating the role of the calling task
//! and the method's semantics.  For each object one task is allowed per role,
//! and the object remembers one waker per role:
//!
//! - `external_poll_*` — for an external process that manages or controls the
//!   object ([`ProcMachine`] and [`AlarmClock`] use this role).  Each such
//!   method documents its own guarantees for the `Ready` case.
//! - `prod_poll_*` — for an item/information *producer*.  These provide no
//!   special behaviour when they return `Ready`.
//! - `con_poll_*` — for an item/information *consumer*.  When a `Ready` return
//!   indicates that something was *consumed* (the call is not idempotent), the
//!   caller's waker is registered to be woken when more might be available —
//!   often by pre-waking before the `Ready` return, where doing so would not
//!   cause an infinite loop.  Zero-item reads and EOF returns are idempotent
//!   and do **not** imply extra wakes.
//! - `alarm_poll_*` — for a task waiting for an event or condition.  Once it
//!   returns `Ready`, further calls keep returning `Ready` until the condition
//!   is reset.
//! - `watch_poll_*` — for a task watching for changes.  The caller's waker is
//!   notified the next time the watched item changes, regardless of whether the
//!   call returned `Ready` or `Pending`.
//!
//! [`examples`]: https://github.com/smallware-io/procmachines/tree/main/examples
extern crate alloc;

pub mod alarm_clock;
pub mod buf_provider;
pub mod input_value;
pub mod intrusive_list;
pub mod io_bytes_exchange;
pub mod io_error;
pub mod io_exchange;
pub mod io_port;
pub mod io_reader;
pub mod io_sink;
pub mod io_stream;
pub mod io_writer;
pub mod proc_machines;
pub mod ref_lockable;
#[cfg(feature = "tokio")]
pub mod tokio_reader_writer;
pub mod wait_loop;
pub mod waker_ref;
pub mod watchable_value;

pub use alarm_clock::*;
pub use buf_provider::*;
pub use input_value::*;
pub use io_bytes_exchange::*;
pub use io_error::*;
pub use io_exchange::*;
pub use io_port::*;
pub use io_reader::*;
pub use io_sink::*;
pub use io_stream::*;
pub use io_writer::*;
pub use proc_machines::*;
pub use ref_lockable::*;
#[cfg(feature = "tokio")]
pub use tokio_reader_writer::*;
pub use wait_loop::*;
pub use waker_ref::*;
pub use watchable_value::*;
