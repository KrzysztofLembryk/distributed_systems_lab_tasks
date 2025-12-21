use tokio::fs as t_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::{path::PathBuf};
use sha2::{Sha256, Digest};
use tokio::io::AsyncSeekExt;
use std::io::SeekFrom;

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};
use crate::storage::recovery_manager::RecoveryManager;
use crate::storage::storage_defs::{SectorRwHashMap, MAX_AVAILABLE_FILE_DESCRIPTORS};
use crate::storage::storage_utils::{create_file_name, create_temp_file_name, scan_dir_and_create_file_name_to_path_map};
use crate::storage::file_descr_manager::{FileDescriptorManager, IoPhase};


#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_recover.rs"]

mod test_stable_sector_manager_recover;

#[cfg(test)]
#[path = "./storage_tests/test_stable_sector_manager_write_crash.rs"]
mod test_stable_sector_manager_write_crash;


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
                
                let file_name = create_file_name(idx, timestamp, writer_rank);
                let file_path = self.root_dir.join(file_name);

                // We take file_descr for our sector from manager, it will take care
                // of opening file, waiting for avaialble descr etc
                let mut f = self.descr_manager
                    .take_file_descr(idx, &file_path, IoPhase::ReadPhase)
                    .await
                    .expect("SecotrsManager::read_data - descr_manager.take_file_descr returned None for ReadPhase, this should never happen");

                let sector_vec = StableSectorManager::read_file_data(&mut f).await;

                // We always need to return file_descr after taking it, and since
                // we read from it, we need to seek its start for next reads to be
                // able to read whole data
                f.seek(SeekFrom::Start(0)).await.expect("SectorsManager::read_data - f.seek start paniced");
                self.descr_manager.give_back_file_descr(idx, f).await;

                return sector_vec;
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


        // take_file_descr will reserve space for us 
        // If file decsriptor was present, it will return it for us, if it wasnt
        // it won't create one since in WritePhase we don't need to do that
        let f = self.descr_manager
            .take_file_descr(
                sector_idx, 
                &tmp_file_path, 
                IoPhase::WritePhase
            ).await;

        // If descr was returned we need to drop it immediately since now we will 
        // open tmp file. And we are allowed to have only ONE file descriptor open 
        // at given time, since that is the number we reserved in descr_manager
        match f
        {
            Some(f) => {
                drop(f);
            }
            None => {

            }
        }

        // save_data_to_file closes all descriptors it opened
        StableSectorManager::save_data_to_file(
            data_to_write.as_slice(), 
            &tmp_file_path, 
            &self.root_dir)
        .await;

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
                // We don't have any file descriptor open, so we can safely use
                // remove_file function
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

        // We don't have any descr open so we can safely use save_data_to_file
        StableSectorManager::save_data_to_file(data_to_write.as_slice(), &dest_file_path, &self.root_dir).await;

        // #########################################################################
        // ######################### REMOVING TEMP FILE ############################
        // #########################################################################
        // - If we crash before removing tmp_file, we recover from tmp_file, and 
        // overwrite new_file
        // - if we crash during removing tmp_file we delete tmp_file and new_file 
        // remains

        // We don't have any descr open so we can safely use remove_file
        StableSectorManager::remove_file(&tmp_file_path, &self.root_dir).await;

        // We need to open result file, to give back file descriptor to descr manager
        let res_f = t_fs::File::open(dest_file_path).await.unwrap();

        self.descr_manager.give_back_file_descr(sector_idx, res_f).await;
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
    /// If dir we write to doesn't exist it also *panics*. <br>
    /// It closes all its file descriptors before returning
    async fn save_data_to_file(data: &[u8], path: &PathBuf, root_dir: &PathBuf)
    {
        if data.len() != SECTOR_SIZE
        {
            panic!("StableSectorManager::save_data_to_file - provided data len ({}) is not equal to SECTOR_SIZE ({})", data.len(), SECTOR_SIZE);
        }
        // We do everywhere 'expect' since these are filesystem operations
        // and if they fail we are expected to panic

        // We always overwrite file if it exists
        let mut file = t_fs::File::create(&path)
            .await
            .expect("save_data_to_file - Failed to open file for sync");

        file.write(data)
            .await
            .expect("save_data_to_file - Failed to write file");

        file.sync_data()
            .await
            .expect("save_data_to_file - fdatasync failed on file");

        // At given moment we can have only one file descriptor open
        drop(file);

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
        f: &mut t_fs::File
    ) -> SectorVec
    {
        let sector_vec = read_data_from_descr(f).await;
        return sector_vec;
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
