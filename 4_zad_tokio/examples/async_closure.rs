#![allow(clippy::type_complexity, reason = "Teaching example")]
use std::pin::Pin;

// Generic function that runs the provided async closure:
async fn run_async_closure<Arg, Ret>(mut closure: impl AsyncFnMut(Arg) -> Ret, arg: Arg) -> Ret {
    closure(arg).await
}

// Will not compile, as we would need to specify the type of the future
// async fn run_any_async_closure<Arg, Ret>(
//     closure: Box<dyn AsyncFnOnce(Arg) -> Ret>,
//     arg: Arg,
// ) -> Ret {
//     closure(arg).await
// }

// As of 1.90, this is still the only way to run stored dyn asynchronous closures:
async fn run_boxed_async_closure<Arg, Ret>(
    closure: Box<dyn FnOnce(Arg) -> Pin<Box<dyn Future<Output = Ret>>>>,
    arg: Arg,
) -> Ret {
    closure(arg).await
}

#[tokio::main]
async fn main() {
    let mut x = 0;

    // After 1.85, we can an async closure that captures from its environment:
    let anon_closure = async |arg| {
        x += 1;
        x + arg
    };

    let ret = run_async_closure(anon_closure, 5).await;

    println!("X is now {x}, the async closure returned: {ret}");

    // But this syntax cannot be used to force the future to be `Pin`.
    // Create and store an async closure it to create a normal closure returning an async block:
    let closure: Box<dyn FnOnce(u32) -> Pin<Box<dyn Future<Output = u32>>>> = Box::new(move |x| {
        Box::pin(async move {
            println!("I am inside the async closure: {x}");
            x + 1
        })
    });

    // Pass the async closure to the function:
    let ret = run_boxed_async_closure(closure, 5).await;

    println!("Result of the async closure: {ret}");
}
