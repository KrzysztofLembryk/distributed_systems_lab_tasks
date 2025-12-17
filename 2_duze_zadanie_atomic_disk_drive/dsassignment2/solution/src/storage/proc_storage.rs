use std::{path::PathBuf};
use tokio::fs as t_fs;
use uuid::timestamp;
use std::collections::{HashMap};
use sha2::{Sha256, Digest};
use std::io::ErrorKind;
use std::sync::Arc;

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};
use crate::storage::storage_defs::{TMP_PREFIX, TimeStampType, WriterRankType};
use crate::storage::utils::{create_file_name, extract_data_from_file_name, extract_data_from_temp_file_name, scan_dir_and_create_file_name_to_path_map};

// ################################ FILE NAME FORMAT ################################
// For normal files: "SectorIdx_timestamp_writeRank"
// For temp files: "tmp_checksum_SectorIdx_timestamp_writeRank"
// ############################# INITIAL SECTOR VALUES ##############################
// If a sector was never written, both logical timestamp and the writer rank are 0, 
// and that it contains 4096 zero bytes.
// ############################ STORING SECTORS IN FILES ############################
// We store ONE SECTOR PER FILE, if data we write doesn't have 4096 bytes we append 0
// to it so that it we always store 4096 bytes

// This struct is only used when creating new process/recovering from crash.
// We search through process directory, read all files from it, recover data from
// temp files and create hashmaps with sector idx, file names and file paths
// so that we can spawn tasks for given sector.
// We have files only for sectors that were written to.
struct ProcStorage
{
    root_dir: PathBuf,
    sector_idx_metadata_map: HashMap<SectorIdx, (TimeStampType, WriterRankType)>,
}

impl ProcStorage
{
    pub async fn recover(
        root_dir: PathBuf,
    ) -> ProcStorage
    {
        let file_name_to_path_map = 
            match scan_dir_and_create_file_name_to_path_map(&root_dir).await
            {
                Ok(m) => m,
                Err(e) => panic!("build_stable_storage - find_file_paths_and_names - got 'filesystem operation fails' error: '{}'", e)
            };

        let mut storage = ProcStorage{
            root_dir,
            sector_idx_metadata_map: HashMap::new(),
        };
        storage.do_the_recovery(file_name_to_path_map).await;

        return storage;
    }

    pub fn spawn_task_for_each_sector(&self)
    {

    }

    async fn do_the_recovery(
        &mut self, 
        mut file_name_to_path_map: HashMap<String, PathBuf>
    )
    {
        // we clone here so that we can modify our map inside check_tmp_file
        let map_cloned = file_name_to_path_map.clone();

        for (file_name, file_path) in &map_cloned
        {
            // Only when we have tmp file we need to do anything, either remove it or save its data to file
            if file_name.starts_with(TMP_PREFIX)
            {
                match self.check_tmp_file(
                    file_name, 
                    file_path, 
                    &mut file_name_to_path_map
                ).await
                {
                    Err(_) => (), // this means we got incorrect checksum
                    _ => ()
                }
            }
        }

        // After above loop all tmp files are deleted and we have only data that was
        // not corrupted, and only file_names in our map, so we can create a map with
        // sector_idx as key and (timestamp, worker_rank) as value
        for (file_name, _) in &file_name_to_path_map
        {
            let (sector_idx, timestamp, writer_rank) = 
                extract_data_from_file_name(file_name);
            self.sector_idx_metadata_map
                .insert(sector_idx, (timestamp, writer_rank));
        }
    }

    async fn check_tmp_file(
        &mut self, 
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
        let computed_hash = Sha256::digest(&data);

        if computed_hash.as_slice() != checksum.as_bytes()
        {
            // We will remove tmp file since it was corrupted
            ProcStorage::remove_file(tmp_file_path, &self.root_dir).await;
            file_name_to_path_map.remove(tmp_file_name);

            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Checksum mismatch"
            )); 
        }

        // computed_hash is correct so we need to overwrite dstfile
        // If the checksum is correct, tmpfile was fully written and a crash happened later. In this case the write can be resumed using the data from tmpfile
        let dest_file_name = create_file_name(sector_idx, timestamp, writer_rank);
        let dest_path = self.root_dir.join(&dest_file_name);

        ProcStorage::save_data_to_file(&data, &dest_path, &self.root_dir).await;
        ProcStorage::remove_file(tmp_file_path, &self.root_dir).await;

        file_name_to_path_map.remove(tmp_file_name);
        file_name_to_path_map.insert(dest_file_name, dest_path.clone());

        Ok(())
    }

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

    async fn remove_file(path: &PathBuf, root_dir: &PathBuf)
    {
        // We do everywhere expect since these are filesystem operations
        // and if they fail we are expected to panic

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

    async fn save_data_to_file(data: &[u8], path: &PathBuf, root_dir: &PathBuf)
    {
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

}

/// Creates a new instance of ProcStorage.
async fn build_stable_storage(
    root_storage_dir: PathBuf,
) -> ProcStorage  //-> Box<dyn SectorsManager> 
{
    return ProcStorage::recover(root_storage_dir).await;
}


