use tokio::fs as t_fs;
use std::{path::PathBuf};
use sha2::{Sha256, Digest};

use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};
use crate::storage::utils::{create_file_name, create_temp_file_name, extract_data_from_file_name};

/// RawFileManager struct implements SectorsManager, it takes care of raw writing to 
/// and reading from files. In our implementation one SECTOR has ONE FILE.
pub struct StableFileManager
{
    root_dir: PathBuf,
}

#[async_trait::async_trait]
impl SectorsManager for StableFileManager
{
    /// Returns 4096 bytes of sector data by index. If file for given idx doesn't 
    /// exist it returns 4096 zero bytes. <br>
    /// Function opens root_dir and iterates over all file names, if encounters one
    /// starting with idx it opens and reads it. 
    async fn read_data(&self, idx: SectorIdx) -> SectorVec
    {
        let mut entries = t_fs::read_dir(&self.root_dir).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap()
        {
            let entry_path = entry.path();
            let meta = entry.metadata().await.unwrap();

            if meta.is_file() 
            {
                if let Some(file_name) = entry_path
                                            .file_name()
                                            .and_then(|s| s.to_str())
                {
                    if file_name.starts_with(&format!("{}", idx))
                    {
                        return self.read_file_data(&entry_path).await;
                    }
                }
            }
        }
        // We didnt find sector for given idx, so nobody wrote to it so we return
        // default value
        return SectorVec::new_from_slice(&[0u8; SECTOR_SIZE]);

        // if let Some(file_name) = self.sector_idx_to_file_name_map.get(&idx)
        // {
        //     let file_path = self
        //         .file_name_to_path_map
        //         .get(file_name)
        //         .expect(&format!("SectorsManager::read_data: for sector idx: '{}', file_name: '{}' exists in self.sector_idx_to_file_name_map but does not exist in self.file_name_to_path_map", idx, file_name));

        //     return self.read_file_data(file_path).await;
        // }
        // else
        // {
        //     return SectorVec::new_from_slice(&[0u8; SECTOR_SIZE]);
        // }
    }

    /// Returns timestamp and write rank of the process which has saved this 
    /// data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are 
    /// described there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8)
    {
        let mut entries = t_fs::read_dir(&self.root_dir).await.unwrap();

        while let Some(entry) = entries.next_entry().await.unwrap()
        {
            let entry_path = entry.path();
            let meta = entry.metadata().await.unwrap();

            if meta.is_file() 
            {
                if let Some(file_name) = entry_path
                                            .file_name()
                                            .and_then(|s| s.to_str())
                {
                    if file_name.starts_with(&format!("{}", idx))
                    {
                        let (_, timestamp, writer_rank) = extract_data_from_file_name(file_name);

                        return (timestamp, writer_rank);
                    }
                }
            }
        }

        // No sector file for given id, so nobody did WRITE to it so return defaults
        let timestamp: u64 = 0;
        let writer_rank: u8 = 0;
        return (timestamp, writer_rank);

        // if let Some(file_name) = self.sector_idx_to_file_name_map.get(&idx)
        // {
        //     let (_, timestamp, writer_rank) = Storage::extract_data_from_file_name(file_name);
        //     return (timestamp, writer_rank);
        // }
        // else
        // {
        //     // After recovery we don't have such sector_idx in Storage, thus nobody
        //     // has ever written to it, thus we return (0, 0)
        //     let timestamp: u64 = 0;
        //     let writer_rank: u8 = 0;
        //     return (timestamp, writer_rank);
        // }
    }

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, sector_idx: SectorIdx, sector: &(SectorVec, u64, u8))
    {
        // For every sector we will create a task that will write to it
        // this task will have channel, and will get msgs
        let (data_to_write, timestamp, writer_rank) = sector;
        let data_checksum: String = 
            format!("{:x}", Sha256::digest(data_to_write.as_slice()));
        let tmp_file_name = create_temp_file_name(
            &data_checksum, sector_idx, *timestamp, *writer_rank
        );
        let tmp_file_path = self.root_dir.join(&tmp_file_name);

        // Now we can save tmp file to the disk
        self.save_data_to_file(data_to_write.as_slice(), &tmp_file_path).await;

        let file_name = create_file_name(
            sector_idx, 
            *timestamp, 
            *writer_rank
        );
        let dest_file_path = self.root_dir.join(&file_name);

        self.save_data_to_file(data_to_write.as_slice(), &dest_file_path).await;
        self.remove_file(&tmp_file_path).await;
    }
}

impl StableFileManager
{
    pub fn new(root_dir: PathBuf) -> StableFileManager
    {
        return StableFileManager { root_dir };
    }

    /// Saves exactly SECTOR_SIZE bytes otherwise *panics*. <br>
    /// If file or dir we write to doesn't exist it also *panics*.
    async fn save_data_to_file(&self, data: &[u8], path: &PathBuf)
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
        let dir = t_fs::File::open(&self.root_dir)
            .await
            .expect("save_data_to_file - failed to open directory for sync");
    
        dir.sync_data()
            .await
            .expect("save_data_to_file - fdatasync failed on directory");
    }

    /// Expects to read **exactly** SECTOR_SIZE bytes - otherwise *panics*. <br>
    /// If file for given path does not exist it also *panics*.
    async fn read_file_data(&self, path: &PathBuf) -> SectorVec
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
    async fn remove_file(&self, path: &PathBuf)
    {
        // We do everywhere expect since these are filesystem operations
        // and if they fail we are expected to panic

        // if file doesn't exist remove_file returns NotFound, should we panic?
        t_fs::remove_file(path)
            .await
            .expect("remove_file - remove file system function failed");

        // We need to sync directory 
        let dir = t_fs::File::open(&self.root_dir)
            .await
            .expect("check_tmp_file - failed to open directory for sync");
    
        dir.sync_data()
            .await
            .expect("check_tmp_file - fdatasync failed on directory");
    }
}
