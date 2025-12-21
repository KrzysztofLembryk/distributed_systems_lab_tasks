use tempfile::tempdir;
use ntest::timeout;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::storage::file_descr_manager::{FileDescriptorManager, DescrCollections, IoPhase};
use crate::domain::{SectorIdx};

#[tokio::test]
#[timeout(500)]
async fn test_unused_descr_reclaimed() 
{
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.keep();

    let max_allowed_open_f_descr = 2;
    let descr_manager = FileDescriptorManager::new(max_allowed_open_f_descr);

    let sector_0: u64 = 0;
    let sector_1: u64 = 1;
    let sector_2: u64 = 2;
    let file_0 = format!("file_{}", sector_0);
    let file_1 = format!("file_{}", sector_1);
    let file_2 = format!("file_{}", sector_2);

    let path_0 = root_path.join(file_0);
    let path_1 = root_path.join(file_1);
    let path_2 = root_path.join(file_2);

    let f_0 = descr_manager.take_file_descr(sector_0, &path_0, IoPhase::ReadPhase).await.unwrap();
    descr_manager.take_file_descr(sector_1, &path_1, IoPhase::ReadPhase).await;

    descr_manager.give_back_file_descr(sector_0, f_0).await;

    descr_manager.take_file_descr(sector_2, &path_2, IoPhase::ReadPhase).await;
}

#[tokio::test]
#[timeout(800)]
async fn test_unused_descr_reclaimed_while_waiting_on_semaphore() 
{
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.keep();

    let max_allowed_open_f_descr = 2;
    let descr_manager = Arc::new(FileDescriptorManager::new(max_allowed_open_f_descr));

    let sector_0: u64 = 0;
    let sector_1: u64 = 1;
    let sector_2: u64 = 2;
    let file_0 = format!("file_{}", sector_0);
    let file_1 = format!("file_{}", sector_1);
    let file_2 = format!("file_{}", sector_2);

    let path_0 = root_path.join(file_0);
    let path_1 = root_path.join(file_1);
    let path_2 = root_path.join(file_2);

    let f_0 = descr_manager.take_file_descr(sector_0, &path_0, IoPhase::ReadPhase).await.unwrap();
    descr_manager.take_file_descr(sector_1, &path_1, IoPhase::ReadPhase).await.unwrap();

    let descr_manager_clone = descr_manager.clone();

    // we spawn task that after some time will release file descr, therefore it 
    // should allow another descr take
    let task_handle = tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        descr_manager_clone.give_back_file_descr(sector_0, f_0).await;
    });

    descr_manager.take_file_descr(sector_2, &path_2, IoPhase::ReadPhase).await;

    task_handle.await.unwrap();

    let open_descr = descr_manager.get_nbr_of_open_descr().await;

    assert!(
        open_descr == max_allowed_open_f_descr,
        "open descriptors not equal max allowed, but it should: {} != {}",
        open_descr,
        max_allowed_open_f_descr
    );
}

#[tokio::test]
#[timeout(1000)]
async fn test_many_reads_on_same_sectors() 
{
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.keep();

    let max_allowed_open_f_descr = 2;
    let descr_manager = Arc::new(FileDescriptorManager::new(max_allowed_open_f_descr));

    // (sector_idx, file_name, sleep_ms)
    let task_info: Vec<(SectorIdx, String, u64)> = vec![
        (0, format!("file_{}", 0), 20),
        (1, format!("file_{}", 1), 10),
        (2, format!("file_{}", 2), 5),
    ];

    let mut handles = Vec::new();

    for (sector_idx, file_name, sleep_ms) in task_info 
    {
        let descr_manager_clone = descr_manager.clone();
        let path = root_path.join(file_name);

        handles.push(tokio::spawn(async move {
            for _ in 0..=25 {
                sleep(Duration::from_millis(sleep_ms)).await;
                let f = descr_manager_clone
                    .take_file_descr(sector_idx, &path, IoPhase::ReadPhase)
                    .await
                    .unwrap();
                descr_manager_clone.give_back_file_descr(sector_idx, f).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let open_descr = descr_manager.get_nbr_of_open_descr().await;

    assert!(
        open_descr == max_allowed_open_f_descr,
        "open descriptors not equal max allowed, but it should: {} != {}",
        open_descr,
        max_allowed_open_f_descr
    );
}

#[tokio::test]
#[timeout(6000)]
async fn descriptor_usage_skewed_takes_and_givebacks() {
    use std::time::Instant;

    let start = Instant::now();
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.keep();

    let max_allowed_open_f_descr = 990;
    let descr_manager = Arc::new(FileDescriptorManager::new(max_allowed_open_f_descr));

    let tasks: usize = 1200;
    let mut task_handles = vec![];
    
    for sector_idx in 0..tasks 
    {
        
        let descr_manager = descr_manager.clone();
        let path = root_path.join(format!("file_{}", sector_idx));

        task_handles.push(tokio::spawn(async move {
            let takes_per_sector;
            if sector_idx > 700
            {
                takes_per_sector = 50;
            }
            else
            {
                takes_per_sector = 100;
            }

            for _ in 0..takes_per_sector {
                let f = descr_manager
                    .take_file_descr(sector_idx as u64, &path, IoPhase::ReadPhase)
                    .await
                    .expect("Failed to take file descriptor");
                descr_manager.give_back_file_descr(sector_idx as u64, f).await;
            }
        }));
    }

    for handle in task_handles {
        assert!(handle.await.is_ok());
    }

    // 3. Check that the number of open descriptors does not exceed the limit
    let open_descr = descr_manager.get_nbr_of_open_descr().await;
    println!(
        "Test finished. Number of open descriptors: {} (max allowed: {})",
        open_descr, max_allowed_open_f_descr
    );
    assert!(
        open_descr == max_allowed_open_f_descr,
        "open descriptors not equal max allowed, but it should: {} != {}",
        open_descr,
        max_allowed_open_f_descr
    );

    let duration = start.elapsed();
    println!("Descriptor usage skewed (take/give_back) Test duration: {:.2?}", duration);
}