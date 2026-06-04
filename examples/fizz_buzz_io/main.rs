use std::{
    cell::Cell,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::future::poll_fn;
use parking_lot::Mutex;
use procmachines::*;

pub struct FizzBuzzIO {
    pub input: IoPort<Arc<Mutex<dyn IoStream<Item = u32, Error = ()> + Send>>>,
    pub output: IoPort<Arc<Mutex<dyn IoSink<&'static str, Error = ()> + Send>>>,
}

impl FizzBuzzIO {
    pub fn new() -> Self {
        Self {
            input: IoPort::new(),
            output: IoPort::new(),
        }
    }
}

impl Default for FizzBuzzIO {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FizzBuzzSrc {
    next: Cell<u32>,
}

impl FizzBuzzSrc {
    pub fn new() -> Self {
        Self { next: Cell::new(1) }
    }
}

impl Default for FizzBuzzSrc {
    fn default() -> Self {
        Self::new()
    }
}

impl IoStream for FizzBuzzSrc {
    type Error = ();
    type Item = u32;

    fn con_poll_read(
        &self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        if self.next.get() > 16 {
            Poll::Ready(Ok(None))
        } else {
            let val = self.next.get();
            println!("{}", val);
            self.next.set(val + 1);
            Poll::Ready(Ok(Some(val)))
        }
    }
    fn drop_read(&self) {
        // no-op
    }
}

pub struct FizzBuzzSink {}

impl FizzBuzzSink {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for FizzBuzzSink {
    fn default() -> Self {
        Self::new()
    }
}

impl IoSink<&'static str> for FizzBuzzSink {
    type Error = ();

    fn prod_poll_ready(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn prod_poll_send(
        &self,
        _cx: &mut Context<'_>,
        item: &mut Option<&'static str>,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(s) = item.take() {
            println!("{}", s);
        }
        Poll::Ready(Ok(()))
    }

    fn prod_poll_flush(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn prod_poll_close(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

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

#[tokio::main]
async fn main() {
    let pm = PROC_MACHINE_BUILDER
        .with(fizz_buzz_task)
        .build_std(FizzBuzzIO::new());
    let _ = pm
        .lock_mut()
        .input
        .connect(Arc::new(Mutex::new(FizzBuzzSrc::new())));
    let _ = pm
        .lock_mut()
        .output
        .connect(Arc::new(Mutex::new(FizzBuzzSink::new())));
    poll_fn(|cx| pm.external_poll(cx)).await;
}
