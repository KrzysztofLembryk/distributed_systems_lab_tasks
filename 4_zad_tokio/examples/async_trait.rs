#![allow(clippy::manual_async_fn)]

struct FutureInt5;

// You can define a trait with asynchronous methods like this:
trait FutureInt {
    async fn get(&self) -> i32;

    // This is simply desugared into
    fn desugared_get(&self) -> impl Future<Output = i32>;
}

async fn use_future_int(x: impl FutureInt) -> i32 {
    x.get().await + x.desugared_get().await
}

impl FutureInt for FutureInt5 {
    async fn get(&self) -> i32 {
        5
    }

    fn desugared_get(&self) -> impl Future<Output = i32> {
        async { 5 }
    }
}

// But this is not "dyn compatible", so this will not compile!
// async fn use_dyn_future_int(x: &dyn FutureInt) -> i32 {
//     x.get().await
// }

// To make a dyn compatible trait, we need a trick.
// The `async_trait` macro will always return a concrete type
// as seen in `async_closure.rs`.
// Put the macro on top of trait definition:
#[async_trait::async_trait]
trait DynFutureInt {
    async fn get(&self) -> i32;

    // This is simply desugared into roughly
    // fn desugared_get(&self) -> std::pin::Pin<Box<dyn Future<Output = i32>>;
}

async fn use_dyn_future_int(x: &dyn DynFutureInt) -> i32 {
    x.get().await
}

// Put the macro on top of every trait implementation:
#[async_trait::async_trait]
impl DynFutureInt for FutureInt5 {
    async fn get(&self) -> i32 {
        5
    }
}

#[tokio::main]
async fn main() {
    println!("Async impl trait: {}", use_future_int(FutureInt5).await);
    println!("Async dyn trait: {}", use_dyn_future_int(&FutureInt5).await);
}
