mod definitions;
mod public_test;
mod solution;

use std::env;
use std::process;

fn parse_args() -> definitions::Num {
    let args: Vec<String> = env::args().collect();
    match args.len() {
        1 => 13,
        2 => {
            if let Ok(n) = args.get(1).unwrap().parse() {
                n
            } else {
                println!("Provide an unsigned number as the program argument!");
                process::exit(1);
            }
        }
        _ => {
            println!("Provide only one argument: an initial number to evaluate the conjecture.");
            process::exit(1);
        }
    }
}

fn main() {
    let n = parse_args();
    let total_stop_time = solution::collatz(n);
    println!("The computation for {n} finished after {total_stop_time} iterations");
}

// Sample result of `cargo run`:
//
// Inside Ident(14), idx: 1, value: 13
// Inside Ident(13), idx: 2, value: 40
// Inside Ident(13), idx: 3, value: 20
// Inside Ident(13), idx: 4, value: 10
// Inside Ident(14), idx: 5, value: 5
// Inside Ident(13), idx: 6, value: 16
// Inside Ident(13), idx: 7, value: 8
// Inside Ident(13), idx: 8, value: 4
// Inside Ident(13), idx: 9, value: 2
// Inside Ident(14), idx: 10, value: 1
// The computation for 13 finished after 10 iterations
