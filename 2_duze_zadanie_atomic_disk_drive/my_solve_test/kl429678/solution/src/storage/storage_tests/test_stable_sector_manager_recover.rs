use ntest::timeout;
use tempfile::tempdir;
use std::collections::HashMap;
use std::io;
use std::path::{PathBuf, Path};
use sha2::{Sha256, Digest};

use crate::SectorsManager;
use crate::storage::stable_sector_manager::StableSectorManager;
use crate::domain::{SECTOR_SIZE, SectorIdx};
use crate::storage::storage_utils::{create_temp_file_name, create_file_name};
use crate::storage::storage_defs::{TimeStampType, WriterRankType};

const SECTOR_IDX: SectorIdx = 1;
const TIMESTAMP: u64 = 42;
const WRITER_RANK: u8 = 7;

#[tokio::test]
#[timeout(300)]
async fn test_recover_from_only_tmp_file() 
{
    // given
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // Prepare test data
    let tmp_file_data = [1u8; SECTOR_SIZE];

    // Compute checksum
    let checksum = format!("{:x}", Sha256::digest(&tmp_file_data));

    write_data_to_file(
        &create_temp_file_path(&checksum, SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &tmp_file_data
    ).await;

    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

    check_if_file_deleted(Some(&checksum), SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path).await;

    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), &tmp_file_data);

    let (read_timestamp, read_writer_rank) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(read_timestamp, TIMESTAMP);
    assert_eq!(read_writer_rank, WRITER_RANK);
}

#[tokio::test]
#[timeout(300)]
async fn test_recover_from_only_old_file() 
{
    // given
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // Prepare test data
    let data = [1u8; SECTOR_SIZE];

    // Create tmp file name
    let file_name = create_file_name(SECTOR_IDX, TIMESTAMP, WRITER_RANK);
    let file_path = root_path.join(&file_name);

    tokio::fs::write(&file_path, &data)
        .await
        .expect("Failed to write tmp file");

    // when: recover
    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

    // then: data and metadata should be correct
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), &data);

    let (read_timestamp, read_writer_rank) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(read_timestamp, TIMESTAMP);
    assert_eq!(read_writer_rank, WRITER_RANK);
}

#[tokio::test]
#[timeout(300)]
async fn test_recover_from_ok_tmp_file_and_old_file() 
{
    // tmp_file data should overwrite normal file's data
    // because tmp_file has correct checksum thus crash happened later

    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // SAVE FILE data
    let old_file_data = [1u8; SECTOR_SIZE];

    write_data_to_file(
        &create_file_path(SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &old_file_data
    ).await;

    // SAVE TEMP_FILE data
    let tmp_file_data = [2u8; SECTOR_SIZE];
    let checksum = format!("{:x}", Sha256::digest(&tmp_file_data));

    write_data_to_file(
        &create_temp_file_path(&checksum, SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &tmp_file_data
    ).await;

    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

    // check if tmp_file deleted
    check_if_file_deleted(Some(&checksum), SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path).await;

    // data should be from temp_file 
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), &tmp_file_data);

    let (read_timestamp, read_writer_rank) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(read_timestamp, TIMESTAMP);
    assert_eq!(read_writer_rank, WRITER_RANK);
}

#[tokio::test]
#[timeout(300)]
async fn test_recover_from_ok_tmp_file_with_new_timestamp_and_old_file() 
{
    // tmp_file data should overwrite normal file's data
    // because tmp_file has correct checksum thus crash happened later

    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // SAVE OLD FILE data
    let old_file_data = [1u8; SECTOR_SIZE];

    write_data_to_file(
        &create_file_path(SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &old_file_data
    ).await;

    // SAVE TEMP_FILE data
    let new_timestamp = TIMESTAMP + 1;
    let tmp_file_data = [2u8; SECTOR_SIZE];
    let checksum = format!("{:x}", Sha256::digest(&tmp_file_data));

    write_data_to_file(
        &create_temp_file_path(&checksum, SECTOR_IDX, new_timestamp, WRITER_RANK, root_path), 
        &tmp_file_data
    ).await;

    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

    // Tmp file should be deleted
    check_if_file_deleted(Some(&checksum), SECTOR_IDX, new_timestamp, WRITER_RANK, root_path).await;
    // Old file should be deleted
    check_if_file_deleted(None, SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path).await;

    // data should be from temp_file 
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), &tmp_file_data);

    let (read_timestamp, read_writer_rank) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(read_timestamp, new_timestamp);
    assert_eq!(read_writer_rank, WRITER_RANK);
}

#[tokio::test]
#[timeout(300)]
async fn test_recover_from_corrupted_tmp_file_and_old_file() 
{
    // tmp_file data should overwrite normal file's data
    // because tmp_file has correct checksum thus crash happened later

    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // SAVE FILE data
    let old_file_data = [1u8; SECTOR_SIZE];

    write_data_to_file(
        &create_file_path(SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &old_file_data
    ).await;

    // We calculate checksum for different data than we save
    let tmp_file_data = [2u8; SECTOR_SIZE];
    let checksum = format!("{:x}", Sha256::digest(&tmp_file_data));

    let tmp_file_data = [3u8; SECTOR_SIZE];

    write_data_to_file(
        &create_temp_file_path(&checksum, SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path), 
        &tmp_file_data
    ).await;

    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

    // Tmp file should be deleted
    check_if_file_deleted(Some(&checksum), SECTOR_IDX, TIMESTAMP, WRITER_RANK, root_path).await;

    // data should be from old_file 
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), &old_file_data);

    let (read_timestamp, read_writer_rank) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(read_timestamp, TIMESTAMP);
    assert_eq!(read_writer_rank, WRITER_RANK);
}

#[tokio::test]
#[timeout(500)]
async fn test_recover_many_files_various_states() 
{
    enum FileType
    {
        // New file from tmp_file should remain, tmp_file deleted
        TMP_OK,
        // tmp_file should be deleted, no normal file created
        TMP_CORRUPTED,
        // normal file should remain
        NORMAL_FILE,
        // tmp_file and old_file exist, thus data from tmp_file overwrites old_file
        OLD_FILE
    }

    let file_types = vec![FileType::TMP_OK, FileType::TMP_OK, 
                          FileType::TMP_CORRUPTED, FileType::TMP_CORRUPTED,
                          FileType::NORMAL_FILE, FileType::NORMAL_FILE,
                          FileType::OLD_FILE];
    let root_drive_dir = tempdir().unwrap();
    let root_path = root_drive_dir.path();

    // Store expected data for later verification
    let mut expected_data: HashMap<SectorIdx, (FileType, TimeStampType, WriterRankType, [u8; SECTOR_SIZE])> = HashMap::new();

    for (i, file_type) in file_types.iter().enumerate() 
    {
        let data: [u8; SECTOR_SIZE] = [i as u8; SECTOR_SIZE];
        let checksum = format!("{:x}", Sha256::digest(&data));
        let sector_idx: SectorIdx = i as SectorIdx;
        let timestamp: u64 = i as u64;
        let writer_rank: u8 = i as u8;

        match file_type
        {
            FileType::TMP_OK => {
                write_data_to_file(
                    &create_temp_file_path(&checksum, sector_idx, timestamp, writer_rank, root_path),
                    &data
                ).await;
                expected_data.insert(sector_idx, (FileType::TMP_OK, timestamp, writer_rank, data));
            },
            FileType::TMP_CORRUPTED => {
                // checksum was calculated on different data
                let corrupted_data: [u8; SECTOR_SIZE] = [(i + 1) as u8; SECTOR_SIZE];

                write_data_to_file(
                    &create_temp_file_path(&checksum, sector_idx, timestamp, writer_rank, root_path),
                    &corrupted_data
                ).await;
                expected_data.insert(sector_idx, (FileType::TMP_CORRUPTED, timestamp, writer_rank, data));
            }
            FileType::NORMAL_FILE => {
                write_data_to_file(
                    &create_file_path(sector_idx, timestamp, writer_rank, root_path),
                    &data
                ).await;
                expected_data.insert(sector_idx, (FileType::NORMAL_FILE, timestamp, writer_rank, data));
            },
            FileType::OLD_FILE => {
                write_data_to_file(
                    &create_temp_file_path(&checksum, sector_idx, timestamp, writer_rank, root_path),
                    &data
                ).await;
                expected_data.insert(sector_idx, (FileType::OLD_FILE, timestamp, writer_rank, data));


                let data: [u8; SECTOR_SIZE] = [(i + 1) as u8; SECTOR_SIZE];
                write_data_to_file(
                    &create_file_path(sector_idx, timestamp, writer_rank, root_path),
                    &data
                ).await;
            }
        }
    }

    // --- Recovery ---
    let manager = StableSectorManager::recover(root_path.to_path_buf()).await;

     // --- Validation ---
    for (sector_idx, (file_type, timestamp, writer_rank, expected_data)) in expected_data.iter() {
        match file_type {
            FileType::TMP_OK => {
                // TMP file should be deleted
                let checksum = format!("{:x}", Sha256::digest(expected_data));
                check_if_file_deleted(Some(&checksum), *sector_idx, *timestamp, *writer_rank, root_path).await;

                // Data and metadata should match tmp file
                let read_data = manager.read_data(*sector_idx).await;
                assert_eq!(read_data.as_slice(), expected_data);

                let (read_ts, read_wr) = manager.read_metadata(*sector_idx).await;
                assert_eq!(read_ts, *timestamp);
                assert_eq!(read_wr, *writer_rank);
            }
            FileType::TMP_CORRUPTED => {
                // TMP file should be deleted
                let checksum = format!("{:x}", Sha256::digest(expected_data));
                check_if_file_deleted(Some(&checksum), *sector_idx, *timestamp, *writer_rank, root_path).await;

                // Data and metadata should be zero for this sector
                let read_data = manager.read_data(*sector_idx).await;
                assert_eq!(read_data.as_slice(), &[0u8; SECTOR_SIZE]);

                let (read_ts, read_wr) = manager.read_metadata(*sector_idx).await;
                assert_eq!(read_ts, 0);
                assert_eq!(read_wr, 0);
            }
            FileType::NORMAL_FILE => {
                // Data and metadata should match file, file shouldn't be deleted
                let read_data = manager.read_data(*sector_idx).await;
                assert_eq!(read_data.as_slice(), expected_data);

                let (read_ts, read_wr) = manager.read_metadata(*sector_idx).await;
                assert_eq!(read_ts, *timestamp);
                assert_eq!(read_wr, *writer_rank);
            }
            FileType::OLD_FILE => {
                // tmp file should be deleted
                let checksum = format!("{:x}", Sha256::digest(expected_data));
                check_if_file_deleted(Some(&checksum), *sector_idx, *timestamp, *writer_rank, root_path).await;

                // old file should remain but with updated data
                // Data and metadata should match tmp file
                let read_data = manager.read_data(*sector_idx).await;
                assert_eq!(read_data.as_slice(), expected_data);

                let (read_ts, read_wr) = manager.read_metadata(*sector_idx).await;
                assert_eq!(read_ts, *timestamp);
                assert_eq!(read_wr, *writer_rank);
            }
        }
    }
}

// #################################################################################
// ############################## HELPER FUNCTIONS #################################
// #################################################################################

fn create_temp_file_path(checksum: &str, sector_idx: SectorIdx, timestamp: u64, writer_rank: u8, root_path: &Path) -> PathBuf
{
    // Create tmp file name
    let tmp_file_name = create_temp_file_name(checksum, sector_idx, timestamp, writer_rank);
    let tmp_file_path = root_path.join(&tmp_file_name);
    return tmp_file_path;
}

fn create_file_path(sector_idx: SectorIdx, timestamp: u64, writer_rank: u8, root_path: &Path) -> PathBuf
{
    // Create tmp file name
    let file_name = create_file_name(sector_idx, timestamp, writer_rank);
    let file_path = root_path.join(&file_name);
    return file_path;
}

async fn write_data_to_file(file_path: &PathBuf, data: &[u8; SECTOR_SIZE])
{
    tokio::fs::write(file_path, data)
        .await
        .expect("Failed to write tmp file");
}

async fn open_file(checksum: Option<&str>, sector_idx: SectorIdx, timestamp: u64, writer_rank: u8, root_path: &Path) -> io::Result<()>
{
    let path: PathBuf;
    if let Some(checksum) = checksum
    {
        let tmp_file_name = create_temp_file_name(checksum, sector_idx, timestamp, writer_rank);
        path = root_path.join(&tmp_file_name);
    }
    else
    {
        let file_name = create_file_name(sector_idx, timestamp, writer_rank);
        path = root_path.join(&file_name);
    }

    match tokio::fs::read(path).await
    {
        Ok(_) => return Ok(()),
        Err(e) => return Err(e)
    }
}

/// checksum == Some, checks if TMP_FILE was deleted
/// checksum == None, checks if OLD_FILE was deleted
async fn check_if_file_deleted(
    checksum: Option<&str>, 
    sector_idx: SectorIdx, 
    timestamp: u64, 
    writer_rank: u8, 
    root_path: &Path
)
{
    // After recovery tmp file should be deleted
    let res = open_file(checksum, sector_idx, timestamp, writer_rank, root_path).await;

    if let Err(e) = res {
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
    } else {
        panic!("Expected error, got Ok");
    }
}