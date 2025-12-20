use tokio::fs as t_fs;
use tokio::io::AsyncReadExt;
use std::{path::PathBuf};
use sha2::{Sha256, Digest};

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};
use crate::storage::recovery_manager::RecoveryManager;
use crate::storage::storage_defs::{SectorRwHashMap, MAX_AVAILABLE_FILE_DESCRIPTORS};
use crate::storage::storage_utils::{create_file_name, create_temp_file_name, scan_dir_and_create_file_name_to_path_map};
use crate::storage::file_descr_manager::{FileDescriptorManager};


#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_recover.rs"]

mod test_stable_sector_manager_recover;

#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_write.rs"]
mod test_stable_sector_manager_write;

enum FileAccess<'a>
{
    FilePath(&'a PathBuf),
    FileDescr(&'a mut tokio::fs::File)
}

// ################################ FILE NAME FORMAT ################################
// For normal files: "SectorIdx_timestamp_writeRank"
// For temp files: "tmp_checksum_SectorIdx_timestamp_writeRank"
// ############################# INITIAL SECTOR VALUES ##############################
// If a sector was never written, both logical timestamp and the writer rank are 0, 
// and that it contains 4096 zero bytes.
// ############################ STORING SECTORS IN FILES ############################
// We store have ONE FILE PER SECTOR. Each sector will have ONE task that manages it.

pub struct StableSectorManager
{
    root_dir: PathBuf,
    sector_map: SectorRwHashMap,
    descr_manager: FileDescriptorManager,
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
                // If we have it in map this means there was WRITE to this sector
                let (timestamp, writer_rank) = *sector_metadata;

                // Explicit read lock drop before reading data from file
                drop(sector_map_reader);

                // Here we need to branch, if we have file descriptor we will use it
                // otherwise we will use file path

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
        
        let sector_map = RecoveryManager::do_the_recovery(
                &root_dir, 
                file_name_to_path_map
            ).await;

        let storage = StableSectorManager{
            root_dir,
            sector_map: SectorRwHashMap::new(sector_map),
            descr_manager: FileDescriptorManager::new(MAX_AVAILABLE_FILE_DESCRIPTORS)
        };

        return storage;
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
    async fn read_file_data<'a>(
        access: FileAccess<'a>
    ) -> (SectorVec, Option<t_fs::File>)
    {
        match access
        {
            FileAccess::FileDescr(f_descr) => {
                let sector_vec = read_data_from_descr(f_descr).await;
                return (sector_vec, None);
            },
            FileAccess::FilePath(path) => {
                // We assume that we get path from self.file_keys, so we know that 
                // file  exists, so this should never panic
                let mut f = match t_fs::File::open(path).await {
                    Ok(d) => d,
                    Err(e) => panic!("read_file_data::File::open - returned error: {e}")
                };
                let sector_vec = read_data_from_descr(&mut f).await;

                return (sector_vec, Some(f));
            }
        }
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

async fn read_data_from_descr(f: &mut t_fs::File) -> SectorVec
{
    let mut data: Vec<u8> = vec![0; SECTOR_SIZE];
    let bytes_read = match f.read(&mut data).await {
        Ok(d) => d,
        Err(e) => panic!("read_file_data::f.read - returned error: {e}")
    };

    if bytes_read != SECTOR_SIZE
    {
        panic!("read_file_data - read data len ({}) is not equalt to SECTOR_SIZE ({}) - this shouldn't have happened", data.len(), SECTOR_SIZE);
    }

    // This should never panic because of above if
    let data_arr: [u8; SECTOR_SIZE] = data.try_into().unwrap();

    return SectorVec::new_from_slice(&data_arr);
}
