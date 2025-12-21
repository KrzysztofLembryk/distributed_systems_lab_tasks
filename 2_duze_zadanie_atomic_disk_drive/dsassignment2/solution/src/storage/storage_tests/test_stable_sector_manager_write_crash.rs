use sha2::{Sha256, Digest};
use std::{path::PathBuf};
use tempfile::tempdir;

use crate::SectorsManager;
use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::storage::storage_utils::{create_file_name, create_temp_file_name};
use crate::storage::stable_sector_manager::StableSectorManager;
use crate::storage::storage_defs::{SectorRwHashMap};
use crate::storage::file_descr_manager::{FileDescriptorManager, IoPhase};

const SECTOR_IDX: SectorIdx = 1;
const TIMESTAMP: u64 = 42;
const WRITER_RANK: u8 = 7;

#[derive(PartialEq)]
enum CrashMoment
{
    BeforeCreateTmpFile,
    BeforeRmvOldFile,
    BeforeSaveNewFile,
    BeforeRmvTmpFile,
}

#[tokio::test]
async fn test_write_crash_before_create_tmp_file() 
{
    let root_dir = tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let mut sector_map = SectorRwHashMap::default();
    let mut descr_manager = FileDescriptorManager::new(10);

    let old_data = [1u8; SECTOR_SIZE];

    write_old_file(&old_data, TIMESTAMP, WRITER_RANK, &root_path, &mut sector_map).await;

    // Simulate crash before creating tmp file
    let new_data = SectorVec::new_from_slice(&[2u8; SECTOR_SIZE]);
    let new_ts = TIMESTAMP + 1;
    let new_wr = WRITER_RANK;
    write(SECTOR_IDX, &(new_data.clone(), new_ts, new_wr), &mut sector_map, &root_path, CrashMoment::BeforeCreateTmpFile, &mut descr_manager).await;

    // Recover
    let manager = StableSectorManager::recover(root_path.clone()).await;

    // Should recover old data
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), old_data.as_slice());
    let (ts, wr) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(ts, TIMESTAMP);
    assert_eq!(wr, WRITER_RANK);
}

#[tokio::test]
async fn test_write_crash_before_remove_old_file() 
{
    let root_dir = tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let mut sector_map = SectorRwHashMap::default();
    let mut descr_manager = FileDescriptorManager::new(10);

    let old_data = [1u8; SECTOR_SIZE];

    write_old_file(&old_data, TIMESTAMP, WRITER_RANK, &root_path, &mut sector_map).await;

    // Simulate crash before removing old file
    let new_data = SectorVec::new_from_slice(&[2u8; SECTOR_SIZE]);
    let new_ts = TIMESTAMP + 1;
    let new_wr = WRITER_RANK;
    write(SECTOR_IDX, &(new_data.clone(), new_ts, new_wr), &mut sector_map, &root_path, CrashMoment::BeforeRmvOldFile, &mut descr_manager).await;

    // Recover
    let manager = StableSectorManager::recover(root_path.clone()).await;

    // Should recover new data (from tmp file)
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), new_data.as_slice());
    let (ts, wr) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(ts, new_ts);
    assert_eq!(wr, new_wr);
}

#[tokio::test]
async fn test_write_crash_before_save_new_file() 
{
    let root_dir = tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let mut sector_map = SectorRwHashMap::default();
    let mut descr_manager = FileDescriptorManager::new(10);

    let old_data = [1u8; SECTOR_SIZE];

    write_old_file(&old_data, TIMESTAMP, WRITER_RANK, &root_path, &mut sector_map).await;

    // Simulate crash before saving new file
    let new_data = SectorVec::new_from_slice(&[2u8; SECTOR_SIZE]);
    let new_ts = TIMESTAMP + 1;
    let new_wr = WRITER_RANK;
    write(SECTOR_IDX, &(new_data.clone(), new_ts, new_wr), &mut sector_map, &root_path, CrashMoment::BeforeSaveNewFile, &mut descr_manager).await;

    // Recover
    let manager = StableSectorManager::recover(root_path.clone()).await;

    // Should recover new data (from tmp file)
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), new_data.as_slice());
    let (ts, wr) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(ts, new_ts);
    assert_eq!(wr, new_wr);
}

#[tokio::test]
async fn test_write_crash_before_remove_tmp_file() 
{
    let root_dir = tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let mut sector_map = SectorRwHashMap::default();
    let mut descr_manager = FileDescriptorManager::new(10);

    let old_data = [1u8; SECTOR_SIZE];

    write_old_file(&old_data, TIMESTAMP, WRITER_RANK, &root_path, &mut sector_map).await;

    // Simulate crash before removing tmp file
    let new_data = SectorVec::new_from_slice(&[2u8; SECTOR_SIZE]);
    let new_ts = TIMESTAMP + 1;
    let new_wr = WRITER_RANK;
    write(SECTOR_IDX, &(new_data.clone(), new_ts, new_wr), &mut sector_map, &root_path, CrashMoment::BeforeRmvTmpFile, &mut descr_manager).await;

    // Recover
    let manager = StableSectorManager::recover(root_path.clone()).await;

    // Should recover new data (from tmp file)
    let read_data = manager.read_data(SECTOR_IDX).await;
    assert_eq!(read_data.as_slice(), new_data.as_slice());
    let (ts, wr) = manager.read_metadata(SECTOR_IDX).await;
    assert_eq!(ts, new_ts);
    assert_eq!(wr, new_wr);
}

async fn write_old_file(
    old_data: &[u8; SECTOR_SIZE], 
    timestamp: u64, 
    writer_rank: u8,
    root_dir: &PathBuf,
    sector_map: &mut SectorRwHashMap
)
{
    // Write old file
    let old_data = SectorVec::new_from_slice(old_data);
    let old_ts = timestamp;
    let old_wr = writer_rank;
    {
        let file_name = create_file_name(SECTOR_IDX, old_ts, old_wr);
        let file_path = root_dir.join(&file_name);
        StableSectorManager::save_data_to_file(old_data.as_slice(), &file_path, &root_dir).await;
        sector_map.write().await.insert(SECTOR_IDX, (old_ts, old_wr));
    }
}

/// Implementation of this function MUST BE EXACTLY THE SAME as in STABLE SECTOR MANAGER
async fn write(
    sector_idx: SectorIdx, sector: &(SectorVec, u64, u8),
    sector_map: &mut SectorRwHashMap,
    root_dir: &PathBuf,
    crash_moment: CrashMoment,
    descr_manager: &mut FileDescriptorManager
)
{
    if crash_moment == CrashMoment::BeforeCreateTmpFile
    {
        return;
    }
    // #########################################################################
    // ########################## SAVING TEMP FILE #############################
    // #########################################################################
    // - If we crash before saving tmp_file we will recover from old_file
    // - If we crash while saving tmp_file, checksum of temp file will be wrong,
    // we will delete tmp_file so only old_file will remain (if it exists)

    let (data_to_write, new_timestamp, new_writer_rank) = sector;
    let data_checksum: String = 
        format!("{:x}", Sha256::digest(data_to_write.as_slice()));

    let tmp_file_name = create_temp_file_name(
        &data_checksum, sector_idx, *new_timestamp, *new_writer_rank
    );
    let tmp_file_path = root_dir.join(&tmp_file_name);


    // take_file_descr will reserve space for us and open or create file for us
    let f = descr_manager.take_file_descr(sector_idx, &tmp_file_path, IoPhase::WritePhase).await;

    // But we need to drop it immediately since now we will open tmp file
    // And we are allowed to have ONE file descriptor open only, since that is 
    // the number we reserved in descr_manager
    match f
    {
        Some(f) => {drop(f)},
        None => {}
    }

    // save_data_to_file closes all descriptors it opened
    StableSectorManager::save_data_to_file(
        data_to_write.as_slice(), 
        &tmp_file_path, 
        root_dir)
    .await;

    if crash_moment == CrashMoment::BeforeRmvOldFile
    {
        return;
    }
    // #########################################################################
    // ######################### REMOVING OLD FILE #############################
    // #########################################################################
    // - If we crash before removing old_file but after successfully saving 
    // tmp_file, StableSectorManager::recover() will see that we have both 
    // tmp_file for sector_idx and old_file, it will recover from tmp_file 
    // and delete old_file
    // - If we crash while removing old file, the same happens as above

    { // scope to prevent holding lock for saving new_data and rmving tmp_file
        let mut sector_map_writer = sector_map.write().await;
        let mut old_file_path: Option<PathBuf> = None;

        if let Some(sector_metadata) = sector_map_writer.remove(&sector_idx)
        {
            // If sector idx is present we remove it, and need to remove its old file
            let (timestamp, writer_rank) = sector_metadata;

            // This should never happen if our outer algorithm works correctly
            if (timestamp, writer_rank) > (*new_timestamp, *new_writer_rank)
            {
                panic!("SectorsManager::write: for sector: '{}' we got (timestamp, writer_rank) > (*new_timestamp, *new_writer_rank) = ({}, {}) > ({}, {})", sector_idx, timestamp, writer_rank, *new_timestamp, *new_writer_rank);
            }

            let old_file_name = create_file_name(
                sector_idx, 
                timestamp, 
                writer_rank
            );
            old_file_path = Some(root_dir.join(&old_file_name));
        }

        // We update/insert new values to map here, so that we don't hold lock long
        sector_map_writer.insert(sector_idx, (*new_timestamp, *new_writer_rank));

        // !!! EXPLICIT DROP !!!
        drop(sector_map_writer);

        if let Some(old_file_path) = old_file_path 
        {
            // We don't have any file descriptor open, so we can safely use
            // remove_file function
            StableSectorManager::remove_file(&old_file_path, root_dir).await;
        }
    }

    if crash_moment == CrashMoment::BeforeSaveNewFile
    {
        return;
    }
    // #########################################################################
    // ########################## SAVING NEW FILE ##############################
    // #########################################################################
    // - If we crash before/during saving new_file but after above steps, we 
    // will recover from tmp_file

    let file_name = create_file_name(
        sector_idx, 
        *new_timestamp, 
        *new_writer_rank
    );
    let dest_file_path = root_dir.join(&file_name);

    // We don't have any descr open so we can safely use save_data_to_file
    StableSectorManager::save_data_to_file(data_to_write.as_slice(), &dest_file_path, root_dir).await;

    if crash_moment == CrashMoment::BeforeRmvTmpFile
    {
        return;
    }
    // #########################################################################
    // ######################### REMOVING TEMP FILE ############################
    // #########################################################################
    // - If we crash before removing tmp_file, we recover from tmp_file, and 
    // overwrite new_file
    // - if we crash during removing tmp_file we delete tmp_file and new_file 
    // remains

    // We don't have any descr open so we can safely use remove_file
    StableSectorManager::remove_file(&tmp_file_path, root_dir).await;

    // We need to open result file, to give back file descriptor to descr manager
    let res_f = tokio::fs::File::open(dest_file_path).await.unwrap();

    descr_manager.give_back_file_descr(sector_idx, res_f).await;
}