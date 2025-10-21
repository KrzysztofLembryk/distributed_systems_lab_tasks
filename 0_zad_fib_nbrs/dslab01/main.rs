mod public_test;
mod solution;

use crate::solution::Fibonacci;
use std::env;
use std::process;

fn parse_arg() -> usize {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: cargo run <index of Fibonacci number>");
        process::exit(1);
    }

    let Ok(n) = args.get(1).unwrap().parse::<usize>() else {
        println!("The provided value cannot be converted to usize!");
        process::exit(1);
    };
    n
}

fn main() {
    let nth = parse_arg();
    println!("Calculating the {nth}-th Fibbonaci number...");

    println!("fibonacci(): {}", Fibonacci::fibonacci(nth));

    let mut fib = Fibonacci::new();
    match fib.nth(nth) {
        Some(num) => println!("iterator: {num}"),
        None => println!("Sorry, the n-th Fibonacci number doesn't fit u128."),
    }
}
