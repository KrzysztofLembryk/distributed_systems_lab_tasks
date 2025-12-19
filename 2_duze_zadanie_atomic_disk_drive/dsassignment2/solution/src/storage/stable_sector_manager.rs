use tokio::fs as t_fs;
use std::collections::HashMap;
use std::{path::PathBuf};
use sha2::{Sha256, Digest};

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};

use crate::storage::storage_defs::{TimeStampType, WriterRankType, TMP_PREFIX, SectorRwHashMap};
use crate::storage::storage_utils::{create_file_name, create_temp_file_name,extract_data_from_temp_file_name, scan_dir_and_create_file_name_to_path_map, create_sector_idx_to_metadata_map};


#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_recover.rs"]

mod test_stable_sector_manager_recover;

#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_write.rs"]
mod test_stable_sector_manager_write;

// ################################ FILE NAME FORMAT ################################
// For normal files: "SectorIdx_timestamp_writeRank"
// For temp files: "tmp_checksum_SectorIdx_timestamp_writeRank"
// ############################# INITIAL SECTOR VALUES ##############################
// If a sector was never written, both logical timestamp and the writer rank are 0, 
// and that it contains 4096 zero bytes.
// ############################ STORING SECTORS IN FILES ############################
// We store have ONE FILE PER SECTOR

pub struct StableSectorManager
{
    root_dir: PathBuf,
    sector_map: SectorRwHashMap
}

#[async_trait::async_trait]
impl SectorsManager for StableSectorManager
{
    /// Returns 4096 bytes of sector data by index. If file for given idx doesn't 
    /// exist it returns 4096 zero bytes. <br>
    /// Function opens root_dir and iterates over all file names, if encounters one
    /// starting with idx it opens and reads it. 
    async fn read_data(&self, idx: SectorIdx) -> SectorVec
    {
        {
            let sector_map_reader = self.sector_map.read().await;

            if let Some(sector_metadata) = sector_map_reader.get(&idx)
            {
                let (timestamp, writer_rank) = *sector_metadata;

                // Explicit read lock drop before reading data from file
                drop(sector_map_reader);

                let file_name = create_file_name(idx, timestamp, writer_rank);
                let file_path = self.root_dir.join(file_name);

                return StableSectorManager::read_file_data(&file_path).await;
            }
        } // sector_map_reader dropped here

        // After recovery we don't have such sector_idx in Storage, thus nobody
        // has ever written to it, thus we return Sector filled with 0
        return SectorVec::new_from_slice(&[0u8; SECTOR_SIZE]);
    }

    /// Returns timestamp and write rank of the process which has saved this 
    /// data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are 
    /// described there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8)
    {
        let timestamp: u64;
        let writer_rank: u8;

        {
            let sector_map_reader = self.sector_map.read().await;

            if let Some(sector_metadata) = sector_map_reader.get(&idx)
            {
                (timestamp, writer_rank) = *sector_metadata;
            }
            else
            {
                timestamp = 0;
                writer_rank = 0;
            }
        } // sector_map_reader dropped here

        return (timestamp, writer_rank);
    }

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, sector_idx: SectorIdx, sector: &(SectorVec, u64, u8))
    {
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
        let tmp_file_path = self.root_dir.join(&tmp_file_name);

        StableSectorManager::save_data_to_file(data_to_write.as_slice(), &tmp_file_path, &self.root_dir).await;

        // #########################################################################
        // ######################### REMOVING OLD FILE #############################
        // #########################################################################
        // - If we crash before removing old_file but after successfully saving 
        // tmp_file, StableSectorManager::recover() will see that we have both 
        // tmp_file for sector_idx and old_file, it will recover from tmp_file 
        // and delete old_file
        // - If we crash while removing old file, the same happens as above

        { // scope to prevent holding lock for saving new_data and rmving tmp_file
            let mut sector_map_writer = self.sector_map.write().await;
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
                old_file_path = Some(self.root_dir.join(&old_file_name));
            }
    
            // We update/insert new values to map here, so that we don't hold lock long
            sector_map_writer.insert(sector_idx, (*new_timestamp, *new_writer_rank));
    
            // !!! EXPLICIT DROP !!!
            drop(sector_map_writer);
    
            if let Some(old_file_path) = old_file_path 
            {
                StableSectorManager::remove_file(&old_file_path, &self.root_dir).await;
            }
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
        let dest_file_path = self.root_dir.join(&file_name);

        StableSectorManager::save_data_to_file(data_to_write.as_slice(), &dest_file_path, &self.root_dir).await;

        // #########################################################################
        // ######################### REMOVING TEMP FILE ############################
        // #########################################################################
        // - If we crash before removing tmp_file, we recover from tmp_file, and 
        // overwrite new_file
        // - if we crash during removing tmp_file we delete tmp_file and new_file 
        // remains
        StableSectorManager::remove_file(&tmp_file_path, &self.root_dir).await;
    }
}

impl StableSectorManager
{
    pub async fn recover(
        root_dir: PathBuf,
    ) -> StableSectorManager
    {
        let file_name_to_path_map = 
            match scan_dir_and_create_file_name_to_path_map(&root_dir).await
            {
                Ok(m) => m,
                Err(e) => panic!("StableSectorManager::recover - scan_dir_and_create_file_name_to_path_map - got 'filesystem operation fails' error: '{}'", e)
            };
        
        let sector_map = StableSectorManager::do_the_recovery(&root_dir, file_name_to_path_map).await;

        let storage = StableSectorManager{
            root_dir,
            sector_map: SectorRwHashMap::new(sector_map),
        };

        return storage;
    }

    async fn do_the_recovery(
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
                match StableSectorManager::recover_from_tmp_file(
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
                        StableSectorManager::remove_old_file_if_exists(
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
        let data = t_fs::read(tmp_file_path).await.expect("check_tmp_file - tokio::fs::read failed");
        let computed_hash = format!("{:x}", Sha256::digest(&data));

        if computed_hash != checksum
        {
            // We will remove tmp file since it was corrupted
            StableSectorManager::remove_file(tmp_file_path, root_dir).await;
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

        StableSectorManager::save_data_to_file(&data, &dest_path, root_dir).await;
        StableSectorManager::remove_file(tmp_file_path, root_dir).await;

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
                StableSectorManager::remove_file(&old_file_path, root_dir).await;
            }
        }
    }

    /// Saves exactly SECTOR_SIZE bytes otherwise *panics*. <br>
    /// If file or dir we write to doesn't exist it also *panics*.
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
    async fn read_file_data(path: &PathBuf) -> SectorVec
    {
        // We assume that we get path from self.file_keys, so we know that file
        // exists
        let data = match t_fs::read(path).await{
            Ok(d) => d,
            Err(e) => panic!("read_file_data - read func returned error: {e}")
        };

        if data.len() != SECTOR_SIZE
        {
            panic!("read_file_data - read data len ({}) is not equalt to SECTOR_SIZE ({}) - this shouldn't have happened", data.len(), SECTOR_SIZE);
        }

        // This should never panic because of above if
        let data_arr: [u8; SECTOR_SIZE] = data.try_into().unwrap();

        return SectorVec::new_from_slice(&data_arr);
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
