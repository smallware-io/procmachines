# Procedural State Machines

It's often advantageous to encapsulate logic in a state machine -- a simple object
with all of its inputs and outputs exposed on the surface.  You can mutate the
inputs and observe how the outputs change.  Having the state machine disconnected
from any I/O or infrastructure makes it easily testable, and allows it to be
deployed in as many contexts as possible.

It's great to *have* a state machine, but it's awful to *write* a state machine.
The requirement to return back out to the caller after every input means that you
can't use the tools of procedural programming in any way that spans those inputs.
You need to use state variables and `switch` statements or equivalent.

But, like many languages these days, rust has an async/await features for I/O
operations, and these features cause the compiler automatically translate your
procedural functions into state machines.

The `procmachines` crate makes it easy to take advantage of this transformation to
produce your own state machines, that you can use as state machines, disconnected
from any I/O or infrastructure.

Features:
- Implement state machines as async functions
- Sans-IO support
- Connect to real I/O instead of writing transfer tasks
- No async runtime required
- No-std support (but one `Arc` is required for initialization)

## Quick start

To create a state machine, you implement an "IO" struct.  Typically, the public
methods and fields provide communication with the state machine caller, while
private methods and fields implement the internal state machine tasks.

ProcMachine tasks are passed `Pin<&IO>` values to access the IO struct, so fields
that the tasks will modify need to support interior mutability.

The IO struct will be owned by an `Arc` (the only required allocation) and guarded
by a `lock_api::Mutex`, so it should be `Send`, but it doesn't need to be `Sync`.

```rust
// examples/fizz_buzz_minimal/main.rs
use std::{
    cell::Cell,
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

use procmachines::*;

#[derive(Debug)]
pub struct FizzBuzzIO {
    input: InputValue<u32>,
    output: Cell<Option<&'static str>>,
}

impl FizzBuzzIO {
    pub fn new() -> Self {
        Self {
            input: InputValue::new(0),
            output: Cell::new(None),
        }
    }
    pub fn set_input(&mut self, value: u32) {
        self.input.set(value);
    }
    pub fn get_output(&self) -> Option<&'static str> {
        self.output.get()
    }
}
```

The state machine itself is implemented as one or more (up to 30) private
async functions.

```rust
async fn fizz_buzz_task(io: Pin<&FizzBuzzIO>) -> TaskEnd {
    let _: () = poll_fn(|cx: &mut Context<'_>| {
        let input = *(io.input.con_poll_ref(cx));
        io.output.set(match input % 15 {
            0 => Some("FizzBuzz"),
            3 | 6 | 9 | 12 => Some("Fizz"),
            5 | 10 => Some("Buzz"),
            _ => None,
        });
        Poll::Pending
    })
    .await;
    TaskEnd()
}
```

To instantiate a state machine, you use `PROC_MACHINE_BUILDER` to bind
an instance of the IO struct to futures for the tasks.

You can then use `lock_mut` or `lock_ref` to access the IO struct through its
`Mutex`.

```rust
pub fn main() {
    let pm = PROC_MACHINE_BUILDER
        .with(fizz_buzz_task)
        .build_std(FizzBuzzIO::new());
    for n in 1..17 {
        pm.lock_mut().set_input(n);
        let out = pm.lock_ref().get_output();
        match out {
            None => println!("{} -> {}", n, n),
            Some(s) => println!("{} -> {}", n, s),
        };
    }
}
```

## Sans-IO

"Sans-IO" is the technique of implementing an I/O protocol as a state machine without
any dependencies on actual I/O infrastructure.  Inputs are pushed into the state machine
and outputs are pulled out, with the actual I/O handled by the calling task.

This crate provides specific support for implementing sans-io state machines:

- The `IoStream`, `IoSink`, `IoReader`, and `IoWriter` traits define asynchronous
  interfaces that are convenient for reading and writing inside procmachines;
- The `IoExchange` and `IoBytesExchange` structs implement both a writer and reader
  on a single-item buffer.

The exchange objects can be used to get data into and out of an IO struct, or for
private communication between processes.  A sans-io version of `FizzBuzzIO` could look
like this:

```rust
// examples/fizz_buzz_sans_io/main.rs
#[derive(Debug)]
pub struct FizzBuzzIO {
    pub input: IoExchange<u32>,
    pub output: IoExchange<&'static str>,
}
```

The tasks that implement a sans-io state machine are procedures that use normal control
flow to move from one "wait loop" to another.

A wait loop is a call to `poll_fn`, or the provided `wait_loop` helper that handles all
of the inputs or outputs that might need to be processed at any specific instant.  Inside
a wait loop, it's usually best to handle the inputs or outputs in "backpressure order":

1. First handle anything that always applies like abort signals;
2. Then send anything that needs sending, if possible;
3. Then try to read input only if there's space available to process it.

For full-duplex communication, with data passing through in both directions,
use a separate task for each direction.

```rust
async fn fizz_buzz_task(io: Pin<&FizzBuzzIO>) -> TaskEnd {
    let mut send1: Option<&'static str> = None;
    let mut send2: Option<&'static str> = None;
    
    let _: () = wait_loop(|cx: &mut Context<'_>| {
        if send1.is_some() {
            return match io.output.prod_poll_send(cx, &mut send1) {
                Poll::Ready(Ok(_)) => Loop::Again,
                Poll::Ready(Err(_)) => Loop::Done(()),
                Poll::Pending => Loop::Wait,
            };
        }
        if send2.is_some() {
            return match io.output.prod_poll_send(cx, &mut send2) {
                Poll::Ready(Ok(_)) => Loop::Again,
                Poll::Ready(Err(_)) => Loop::Done(()),
                Poll::Pending => Loop::Wait,
            };
        }
        match io.input.con_poll_read(cx) {
            Poll::Ready(Ok(Some(val))) => {
                if val % 3 == 0 {
                    send1 = Some("Fizz");
                }
                if val % 5 == 0 {
                    send2 = Some("Buzz");
                }
                Loop::Again
            }
            Poll::Ready(Ok(None)) | Poll::Ready(Err(_)) => Loop::Done(()),
            Poll::Pending => Loop::Wait,
        }
    })
    .await;
    TaskEnd()
}
```

## Connected IO

When using a sans-io state machine in an application, you need external tasks to
shuttle data back and forth between the *real* IO interfaces and the state machine.

Since a procedural state machine has internal tasks calling into IO-like interfaces,
such external tasks are a redundant layer.

Using `IoPort<T>` in your IO object allows the caller to dynamically connect the
IO implementations.  If you connect `IoExchange` or `IoBytesExchange` implementations,
then you can use it in the sans-io style for testing.  Then you can just connect real
IO instead of shuttling data back and forth.

```rust
// examples/fizz_buzz_io/main.rs
pub struct FizzBuzzIO {
    pub input: IoPort<Arc<Mutex<dyn IoStream<Item = u32, Error = ()> + Send>>>,
    pub output: IoPort<Arc<Mutex<dyn IoSink<&'static str, Error = ()> + Send>>>,
}
```

This requires no changes to the state machine tasks, and in the caller, we can connect
these ports however we like.

IMPORTANT: We still need an external task, however.  Since we connected the internal
tasks to external IO, those tasks can be woken by external events.  In order to get
the state machine to progress, we need an external task to poll for external wakes.

```rust
    let _ = pm
        .lock_mut()
        .input
        .connect(Arc::new(Mutex::new(FizzBuzzSrc::new())));
    
    let _ = pm
        .lock_mut()
        .output
        .connect(Arc::new(Mutex::new(FizzBuzzSink::new())));

    poll_fn(|cx| pm.external_poll(cx)).await;
```

## Clocks and Timeouts

In a sans-IO state machine, time is an input that needs to be pushed in.  This crate
provides the `AlarmClock` and `ClockAlarm` structs to help with this:

- `AlarmClock` maintains the current time.  It is set by an external process
  calling `advance(new_time)`.
- Any number of `ClockAlarm` structs can be created on an `AlarmClock`, each one
  with a different (optional) alarm time.  Each `ClockAlarm` must have a single
  owning task that can call `alarm_poll` to wait for the clock time to exceed the
  alarm time.
- The external process that provides the time can call `AlarmClock::external_poll_new_alarm`
  to be notified when an alarm is set with an alarm time earlier than a given
  threshold value.  In combination with `advance`, this allows the external process
  to implement an appropriate strategy for timing clock updates to service
  alarms.

## Polling Roles and Semantics

This crate contains many pollable structs and traits.  These are polled much like
`Future`, but with some extended features:

- Some objects can be polled by multiple tasks.  `IoExchange` is used by both a
  "producer" task and a "consumer" task, to communicate items from the producer
  to the consumer.
- Most objects can be polled many times, and return `Ready` many times.  When they
  return `Pending` it has the same implications as when a `Future` returns `Pending`,
  but when they return `Ready`, they will often still register the callers waker
  to be notified the next time any information they consumed is changed.

The names of all polling methods have a prefix, which indicates the role of the
task that calls it, and its semantics.  For each object, one task is allowed per
role, and the polled object will have remember one waker for each role.

The types of polling methods used in this crate are:

- `external_poll_*` for use by an external process that manages or controls the
  object. `ProcMachine` and `AlarmClock` use this role.  Each polling method
  provides unique guarantees for behaviour when `Ready` is returned.
- `prod_poll_*` for use by an information or item "producer".  These methods
  do not provide any special behaviour when they return `Ready`.
- `con_poll_*` for use by an information or item "consumer".  When a `Ready`
  return from one of these methods indicates that something has been "consumed",
  i.e. the method is not idempotent, then the calling task's waker will be
  registered to be awoken when more things might be available for consumption.
  Often this means that the task will be pre-woken before the ready return, when
  this wouldn't cause an infinite loop.
  Note that 0-item reads and EOF returns ARE idempotent and do not imply extra
  wakes.
- `alarm_poll_*` for use by a task that is waiting for an event or condition.
  Once the method returns `Ready`, it is expected that any further calls will
  immediately return `Ready` again until the alarm condition is reset.
- `watch_poll_*` for use by a task that is watching for changes.  When a task
  calls one of these methods, its waker will be notified the next time the
  watched item changes, no matter what the method returns (`Ready` or `Pending`).

## License

[MIT](LICENSE)

