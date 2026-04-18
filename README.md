# procmachines

Procedural state machines driven by cooperative async tasks.

Write protocol logic as straight-line `async` code — loops, branches, `.await` — while
retaining the testability and control of a traditional sans-IO state machine.

## Why

Implementing a protocol as an explicit state machine (an enum of states, a `poll` driver,
manual save/restore of locals across yield points) is correct but painful to write and
read. `procmachines` lets you keep the same mental model while writing the logic
procedurally:

- **No runtime dependency.** Tasks are polled synchronously under a single mutex. There
  is no executor, no spawning, and no hidden concurrency.
- **Deterministic scheduling.** All tasks share one lock, so the polling order is fully
  controlled and there are no data races between tasks.
- **External clock.** Timeouts use an `AlarmClock` advanced by the caller, so protocols
  are testable without real wall-clock time.

## Quick start

```rust
use procmachines::*;
use std::pin::Pin;
use std::sync::Arc;

// 1. Define the IO struct that external code and tasks share.
#[derive(Debug)]
struct MyIO {
    exchange: IoExchange<Vec<u8>>,
    // ... other fields
}

// 2. Write tasks as async functions over a pinned reference to the IO.
async fn reader(io: Pin<&'static MyIO>) -> TaskEnd {
    loop {
        let item = io.exchange.read().await;
        // process item ...
    }
}

// 3. Build the machine.
let machine: Arc<dyn ProcMachine<MyIO>> = PROC_MACHINE_JOBS_BASE
    .with(reader)
    .build(MyIO { exchange: IoExchange::new() });

// 4. Interact through the guard.
{
    let mut guard = machine.lock();   // acquires the mutex
    guard.exchange.send(vec![1, 2, 3]);
}   // dropping the guard ticks the machine — tasks run until they all block
```

## How it works

A `ProcMachine` bundles one or more `async fn` tasks together with a shared IO struct
inside an `Arc`. External code calls `lock()` to obtain an `IoGuard` that derefs to the
IO struct. When the guard is dropped, the machine *ticks*: every task whose waker has
fired is polled until all tasks return `Pending` (or complete).

```text
 External code               ProcMachine (behind Arc)
┌────────────┐      lock()  ┌──────────────────────────────┐
│            │─────────────>│  Mutex<ProcMachineInner>     │
│            │  IoGuard     │  ┌────────────────────────┐  │
│ reads/writes<────────────>│  │ IO struct              │  │
│ to IO      │  Deref       │  ├────────────────────────┤  │
│            │              │  │ Future 1 (task A)      │  │
│            │<─────────────│  │ Future 2 (task B)      │  │
│            │  drop guard  │  │ ...                    │  │
│            │  → tick()    │  │ alive_mask: u32        │  │
└────────────┘              │  └────────────────────────┘  │
                            │  wake_mask: AtomicU32        │
                            └──────────────────────────────┘
```

Tasks communicate with each other and with external code exclusively through the shared IO
struct, using the provided primitives:

| Primitive | Purpose |
|---|---|
| `IoExchange<T>` | Single-slot rendezvous channel (implements `IoSink` + `IoStream`) |
| `AlarmClock<T>` / `ClockAlarm` | Timeout management with an externally driven clock |
| `WatchableValue<T>` / `ValueWatch` | Async notification when a value changes |

## Compile-time task composition

Tasks are assembled at compile time via a builder starting from `PROC_MACHINE_JOBS_BASE`.
Each `.with(task_fn)` call extends a type-level linked list — there is no dynamic dispatch,
no trait objects, and no allocation per task. The resulting poll loop is a static chain of
monomorphised function calls.

Up to 31 concurrent tasks are supported (one bit per task in a `u32` mask).

## License

[MIT](LICENSE)

