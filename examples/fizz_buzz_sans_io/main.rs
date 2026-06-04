use std::{
    pin::Pin,
    task::{Context, Poll},
};

use procmachines::*;

#[derive(Debug)]
pub struct FizzBuzzIO {
    pub input: IoExchange<u32>,
    pub output: IoExchange<&'static str>,
}

impl FizzBuzzIO {
    pub fn new() -> Self {
        Self {
            input: IoExchange::new(),
            output: IoExchange::new(),
        }
    }
}

impl Default for FizzBuzzIO {
    fn default() -> Self {
        Self::new()
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
    let mut next_int = 1;
    let _ = wait_loop(|cx| {
        let io = pm.lock_ref();
        match io.output.con_poll_read(cx) {
            Poll::Ready(Ok(Some(s))) => {
                println!("{}", s);
                return Loop::Again;
            }
            Poll::Ready(_) => return Loop::Done(()),
            Poll::Pending => (),
        };
        if next_int > 16 {
            return Loop::Done(());
        }
        match io.input.prod_poll_send(cx, &mut Some(next_int)) {
            Poll::Ready(Ok(_)) => {
                println!("{}", next_int);
                next_int += 1;
                return Loop::Again;
            }
            Poll::Ready(_) => return Loop::Done(()),
            Poll::Pending => {}
        }
        Loop::Wait
    })
    .await;
}
