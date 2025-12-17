use tokio::fs as t_fs;
use std::{path::PathBuf};
use std::collections::{HashMap};

use crate::domain::{SectorIdx};
use crate::storage::storage_defs::{TimeStampType, WriteRankType, TMP_PREFIX};

pub fn extract_data_from_temp_file_name(
    tmp_file_name: &str
) -> (SectorIdx, TimeStampType, WriteRankType, &str)
{
    // We expect this function to be called onlt if sb checked that temp_file
    // starts with 'tmp_'

    let mut iter = tmp_file_name.split_terminator('_');
    let tmp_pref = iter.next().expect("extract_data_from_file_path: tmp_pred is None, temp_file was incorrect");

    if tmp_pref != "tmp"
    {
        panic!("extract_data_from_temp_file_path: tmp_pref: '{}' != tmp, we are in corrupted state, PANICKING", tmp_pref);
    }

    let checksum: &str = iter.next().expect("extract_data_from_temp_file_path: checksum is None, temp_file was incorrect");

    let mut file_path: String = String::new();
    for val in iter
    {
        file_path.push_str(val);
        file_path.push('_');
    }
    if file_path.is_empty()
    {
        panic!("extract_data_from_temp_file_path: created file_path is empty");
    }

    file_path.remove(file_path.len() - 1);

    let (sector_idx, timestamp, write_rank) = extract_data_from_file_name(&file_path);

    return (sector_idx, timestamp, write_rank, checksum);
}

pub fn extract_data_from_file_name(
    file_name: &str
) -> (SectorIdx, TimeStampType, WriteRankType)
{
    let mut iter = file_name.split_terminator('_');

    let sector_idx: SectorIdx = iter
        .next()
        .expect("extract_data_from_file_path: sector_idx is None, temp_file was incorrect")
        .parse()
        .expect("extract_data_from_file_path: sector_idx: value couldn't be  parsed");
    let timestamp: u64 = iter
        .next()
        .expect("extract_data_from_file_path: timestamp is None, temp_file was incorrect")
        .parse()
        .expect("extract_data_from_file_path: timestamp: value couldn't be  parsed");
    let write_rank: u8 = iter
        .next()
        .expect("extract_data_from_file_path: write_rank is None, temp_file was incorrect")
        .parse()
        .expect("extract_data_from_file_path: write_rank: value couldn't be  parsed");

    return (sector_idx, timestamp, write_rank);
}

pub fn create_temp_file_name(
    checksum: &str, 
    sector_idx: SectorIdx, 
    timestamp: TimeStampType, 
    writer_rank: WriteRankType
) -> String
{
    return format!("{}_{}_{}_{}_{}", TMP_PREFIX, checksum, sector_idx, timestamp, writer_rank);
}

pub fn create_file_name(
    sector_idx: SectorIdx, 
    timestamp: TimeStampType, 
    writer_rank: WriteRankType
) -> String
{
    return format!("{}_{}_{}", sector_idx, timestamp, writer_rank);
}

/// Function scans directory and inserts all file_name : file_path to hashmap
pub async fn scan_dir_and_create_file_name_to_path_map(
    dir_path: &PathBuf
) -> std::io::Result<HashMap<String, PathBuf>> 
{
    let mut file_name_to_path_map: HashMap<String, PathBuf> = HashMap::new();

    // we panic when filesystem function fails
    let mut entries = t_fs::read_dir(dir_path).await.unwrap();

    while let Some(entry) = entries.next_entry().await?
    {
        let entry_path = entry.path();
        let meta = entry.metadata().await?;

        if meta.is_file() 
        {
            // if file path ends with '.' or '..' it returns None 
            if let Some(file_name) = entry_path
                                        .file_name()
                                        .and_then(|s| s.to_str())
            {
                file_name_to_path_map.insert(file_name.to_string(), entry_path);
            }
        }
    }

    Ok(file_name_to_path_map)
}