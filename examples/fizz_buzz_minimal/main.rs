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

impl Default for FizzBuzzIO {
    fn default() -> Self {
        Self::new()
    }
}

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
