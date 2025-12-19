use tokio::fs as t_fs;
use tokio::io::AsyncReadExt;
use std::collections::{HashMap};
use std::{path::PathBuf};
use sha2::{Sha256, Digest};

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};

use crate::storage::storage_defs::{TimeStampType, WriterRankType, TMP_PREFIX};
use crate::storage::storage_utils::{create_file_name, extract_data_from_temp_file_name, create_sector_idx_to_metadata_map};

// During recovery phase of our StableSectorManager we don't want use semaphores
// and storing opened file descriptors, this would overcomplicate things, thus we
// have RecoveryPhase struct with simplified methods that do read/write/remove
pub struct RecoveryManager {}

impl RecoveryManager
{
    pub async fn do_the_recovery(
        root_dir: &PathBuf,
        mut file_name_to_path_map: HashMap<String, PathBuf>
    ) -> HashMap<SectorIdx, (TimeStampType, WriterRankType)>
    {
        // we clone here so that we can modify our map inside check_tmp_file
        let map_cloned = file_name_to_path_map.clone();
        
        // We need map that stores ALL NOT TEMP FILE NAMES, since the way we do WRITE
        // is:
        // - we firstly create a temp_file with new data, 
        // - then after successful temp_file save, we delete old_file 
        // - then after successful delete we create new_file (it might have 
        //   different name than the old file since timestamp/writer_rank may have 
        //   changed)
        // - then we remove temp_file
        //
        // So at any given moment we have at most ONE NOT TEMP FILE for given sector
        // but, if there was a crash, it might be an old_file
        // So if recovery from tmp_file is successful we need to remove old_file
        // but only if (old_timestamp, w_rank) != (new_timestamp, w_rank)
        // because if it equals, our tmp_file overwritten old_file so its OK
        let ignore_tmp_files = true;
        let sector_idx_metadata_of_non_tmp_files = 
            create_sector_idx_to_metadata_map(
                &file_name_to_path_map, 
                ignore_tmp_files
        );

        for (file_name, file_path) in &map_cloned
        {
            // Only when we have tmp file we need to do anything, either remove it or save its data to file
            if file_name.starts_with(TMP_PREFIX)
            {
                match RecoveryManager::recover_from_tmp_file(
                    root_dir,
                    file_name, 
                    file_path, 
                    &mut file_name_to_path_map
                ).await
                {
                    Err(_) => {
                        // We got incorrect checksum, tmp_file has been deleted
                        // If old_file exists we will use it
                    }, 
                    _ => {
                        // Checksum was correct, so tmp_file has been deleted, but 
                        // new_file has also been created.
                        // So we need to check if file for sector_idx is present
                        // in sector_idx_metadata_map, if it is it means that there
                        // is an old_file and we need to delete it 
                        RecoveryManager::remove_old_file_if_exists(
                            file_name, 
                            root_dir, 
                            &sector_idx_metadata_of_non_tmp_files, 
                            &mut file_name_to_path_map
                        ).await;
                    }
                }
            }
        }

        // After above loop all tmp files are deleted and we have only data that was
        // not corrupted, and only file_names in our map, so we can create a map with
        // sector_idx as key and (timestamp, worker_rank) as value
        let ignore_tmp_files = false;
        let sector_idx_to_metadata_map = 
            create_sector_idx_to_metadata_map(
                &file_name_to_path_map, 
                ignore_tmp_files
        );

        return sector_idx_to_metadata_map;
    }

    async fn recover_from_tmp_file(
        root_dir: &PathBuf,
        tmp_file_name: &String,
        tmp_file_path: &PathBuf,
        file_name_to_path_map: &mut HashMap<String, PathBuf>
    ) -> std::io::Result<()>
    {
        if !tmp_file_name.starts_with(TMP_PREFIX)
        {
            panic!("check_tmp_file was given a file name: '{tmp_file_name}' without 'tmp' prefix, this might cause corruption");
        }

        let (sector_idx, timestamp, writer_rank, checksum) =    
            extract_data_from_temp_file_name(&tmp_file_name);
        // We cannot 
        let data = RecoveryManager::read_file_data(tmp_file_path).await;
        let computed_hash = format!("{:x}", Sha256::digest(&data));

        if computed_hash != checksum
        {
            // We will remove tmp file since it was corrupted
            RecoveryManager::remove_file(tmp_file_path, root_dir).await;
            file_name_to_path_map.remove(tmp_file_name);

            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch"
            )); 
        }

        // computed_hash is correct so we need to overwrite dstfile
        // If the checksum is correct, tmpfile was fully written and a crash happened later. In this case the write can be resumed using the data from tmpfile
        let dest_file_name = create_file_name(sector_idx, timestamp, writer_rank);
        let dest_path = root_dir.join(&dest_file_name);

        RecoveryManager::save_data_to_file(&data, &dest_path, root_dir).await;
        RecoveryManager::remove_file(tmp_file_path, root_dir).await;

        file_name_to_path_map.remove(tmp_file_name);
        file_name_to_path_map.insert(dest_file_name, dest_path.clone());

        Ok(())
    }

    async fn remove_old_file_if_exists(
        tmp_file_name: &str,
        root_dir: &PathBuf,
        sector_idx_metadata_map:&HashMap<SectorIdx, (TimeStampType, WriterRankType)>,
        file_name_to_path_map: &mut HashMap<String, PathBuf>
    )
    {
        let (sector_idx, new_timestamp, new_writer_rank, _) = 
            extract_data_from_temp_file_name(tmp_file_name);

        if let Some(meta) = sector_idx_metadata_map.get(&sector_idx)
        {
            let (old_timestamp, old_writer_rank) = *meta;

            if (old_timestamp, old_writer_rank) 
                != (new_timestamp, new_writer_rank) 
            {
                let old_file_name = create_file_name(
                    sector_idx, 
                    old_timestamp, 
                    old_writer_rank
                );
                let old_file_path = root_dir.join(&old_file_name);

                file_name_to_path_map.remove(&old_file_name);
                RecoveryManager::remove_file(&old_file_path, root_dir).await;
            }
        }
    }

    async fn save_data_to_file(data: &[u8], path: &PathBuf, root_dir: &PathBuf)
    {
        if data.len() != SECTOR_SIZE
        {
            panic!("RawFileManager::save_data_to_file - provided data len ({}) is not equal to SECTOR_SIZE ({})", data.len(), SECTOR_SIZE);
        }
        // We do everywhere 'expect' since these are filesystem operations
        // and if they fail we are expected to panic

        // Write either creates or overwrites existing file
        t_fs::write(&path, data)
            .await
            .expect("save_data_to_file - Failed to write file");

        // We sync file to transfer it to disk
        let file = t_fs::File::open(&path)
            .await
            .expect("save_data_to_file - Failed to open file for sync");

        file.sync_data()
            .await
            .expect("save_data_to_file - fdatasync failed on file");

        // We need to sync directory to ensure write happended
        let dir = t_fs::File::open(root_dir)
            .await
            .expect("save_data_to_file - failed to open directory for sync");
    
        dir.sync_data()
            .await
            .expect("save_data_to_file - fdatasync failed on directory");
    }

    /// Expects to read **exactly** SECTOR_SIZE bytes - otherwise *panics*. <br>
    /// If file for given path does not exist it also *panics*.
    async fn read_file_data(
        file_path: &PathBuf
    ) -> Vec<u8>
    {
        let data = t_fs::read(file_path).await.expect("RecoveryPhase:: tokio::fs::read failed");
        return data;
    }

    /// *Panics* if file or directory doesn't exist
    async fn remove_file(path: &PathBuf, root_dir: &PathBuf)
    {
        // We do everywhere expect since these are filesystem operations
        // and if they fail we are expected to panic

        // if file doesn't exist remove_file returns NotFound, should we panic?
        t_fs::remove_file(path)
            .await
            .expect("remove_file - remove file system function failed");

        // We need to sync directory 
        let dir = t_fs::File::open(root_dir)
            .await
            .expect("check_tmp_file - failed to open directory for sync");
    
        dir.sync_data()
            .await
            .expect("check_tmp_file - fdatasync failed on directory");
    }
}