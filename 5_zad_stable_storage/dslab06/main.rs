use std::path::PathBuf;
use tokio::time::{Duration, sleep};
mod public_test;
mod solution;

#[tokio::main]
async fn main() 
{
    create_storage().await;
}

async fn create_storage()
{
    // let root_storage_dir = PathBuf::from("./stable_storage_data");
    
    let root_storage_dir = std::env::temp_dir().join("stable_storage_data");
    tokio::fs::create_dir(&root_storage_dir).await.unwrap();

    {
        let mut storage = solution::build_stable_storage(root_storage_dir.clone()).await;
        storage.put("key", "value".as_bytes()).await.unwrap();
    } // "crash"

    // sleep(Duration::from_secs(10)).await;
    {
        let storage = solution::build_stable_storage(root_storage_dir.clone()).await;
        let value = String::from_utf8(storage.get("key").await.unwrap()).unwrap();
        println!("Recovered value: '{value}'");

        let mut storage = storage;
        let removed = storage.remove("key").await;
        println!("Removed the value? {removed:?}");
    }

    tokio::fs::remove_dir_all(root_storage_dir).await.unwrap();
}