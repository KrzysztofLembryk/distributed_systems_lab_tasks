use std::{path::PathBuf};
use tokio::fs as t_fs;
use std::collections::{HashMap};
use sha2::{Sha256, Digest};
use std::io::ErrorKind;
use crate::domain::{SECTOR_SIZE};

const KEY_MAX_SIZE: usize = 255;
const HASH_TAG_SIZE: usize = 32;
const TMP_PREFIX: &str = "tmp";

type SectorIdx = u64;
type TimeStampType = u64;
type WriteRankType = u8;

// ################################ FILE NAME FORMAT ################################
// For normal files: "SectorIdx_timestamp_writeRank"
// For temp files: "tmp_checksum_SectorIdx_timestamp_writeRank"
// ############################# INITIAL SECTOR VALUES ##############################
// If a sector was never written, both the logical timestamp and the 
// write rank are 0, and that it contains 4096 zero bytes.

struct Storage
{
    root_dir: PathBuf,
    file_keys: HashMap<String, PathBuf>,
    n_sectors: u64,
}

impl Storage
{
    pub fn new(
        keys: HashMap<String, PathBuf>, 
        root_dir: PathBuf,
        n_sectors: u64,
    ) -> Storage
    {
        Storage{
            root_dir,
            file_keys: keys,
            n_sectors,
        }
    }

    pub async fn recover(&mut self)
    {
        // we clone here so that we can modify our map inside check_tmp_file
        let keys = self.file_keys.clone();

        for (file_name, file_path) in &keys
        {
            // Only when we have tmp file we need to do anything, either remove it or save its data to file
            if file_name.starts_with(TMP_PREFIX)
            {
                match self.check_tmp_file(file_name, file_path).await
                {
                    Err(_) => (), // this means we got incorrect checksum
                    _ => ()
                }
            }
        }
    }

    async fn check_tmp_file(
        &mut self, 
        tmp_file_name: &String,
        tmp_file_path: &PathBuf
    ) -> std::io::Result<()>
    {
        if !tmp_file_name.starts_with(TMP_PREFIX)
        {
            panic!("check_tmp_file was given a file name: '{tmp_file_name}' without 'tmp_' prefix, this might cause corruption");
        }

        Storage::extract_data_from_temp_file_name(&tmp_file_name);

        let data = t_fs::read(tmp_file_path).await.expect("check_tmp_file - tokio::fs::read failed");

        // We need to check if data is at most SECTOR_SIZE + HASH_SIZE, if our 
        // programme runs correctly this will always be true, but if sb tempered
        // with our storage this might happen
        if data.len() != SECTOR_SIZE 
        {
            panic!("check_tmp_file - File: '{tmp_file_name}' size ({}) is not equal to SECTOR_SIZE ({}), this shouldn't have happened", data.len(), SECTOR_SIZE);
        }

        let computed_hash = Sha256::digest(&data);

        if computed_hash.as_slice() != file_hash
        {
            // We will remove tmp file since it was corrupted
            self.remove_file(tmp_file_path).await;
            self.file_keys.remove(tmp_file_name);

            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Checksum mismatch"
            )); 
        }

        // computed_hash is correct so we need to overwrite dstfile
        // If the checksum is correct, tmpfile was fully written and a crash happened later. In this case the write can be resumed using the data from tmpfile
        let dest_file_name = tmp_file_name.strip_prefix(TMP_PREFIX).unwrap();
        let dest_path = self.root_dir.join(dest_file_name);

        self.save_data_to_file(file_data, &dest_path).await;
        self.remove_file(tmp_file_path).await;

        self.file_keys.remove(tmp_file_name);
        self.file_keys.insert(dest_file_name.to_string(), dest_path.clone());

        Ok(())
    }

    async fn read_file_data(&self, path: &PathBuf) -> Vec<u8>
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

        data
    }

    async fn remove_file(&mut self, path: &PathBuf)
    {
        // We do everywhere expect since these are filesystem operations
        // and if they fail we are expected to panic

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

    async fn save_data_to_file(&self, data: &[u8], path: &PathBuf)
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
        let dir = t_fs::File::open(&self.root_dir)
            .await
            .expect("save_data_to_file - failed to open directory for sync");
    
        dir.sync_data()
            .await
            .expect("save_data_to_file - fdatasync failed on directory");
    }

    fn get_empty_sector_data() -> (TimeStampType, WriteRankType, Vec<u8>)
    {
        let timestamp: TimeStampType = 0;
        let write_rank: WriteRankType = 0;
        let data: Vec<u8> = vec![0; SECTOR_SIZE];

        return (timestamp, write_rank, data);
    }

    fn extract_data_from_temp_file_name(
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
        // this might panic if sb supplies i.e. "tmp_checksum"
        file_path.remove(file_path.len() - 1);

        let (sector_idx, timestamp, write_rank) = Storage::extract_data_from_file_name(&file_path);

        return (sector_idx, timestamp, write_rank, checksum);
    }

    fn extract_data_from_file_name(
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

}

#[async_trait::async_trait]
impl StableStorage for Storage
{
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>
    {
        if key.len() > KEY_MAX_SIZE
        {
            return Err(format!("Provided key: '{}' ({} bytes) is longer than allowed 255 bytes", key, key.len()));
        }
        if value.len() > SECTOR_SIZE
        {
            return Err(format!("Provided value: '{}' bytes is longer than allowed {} bytes", value.len(), SECTOR_SIZE));
        }
        let mut padded_value: Vec<u8> = value.to_vec();
        if value.len() < SECTOR_SIZE
        {
            padded_value.resize(SECTOR_SIZE, 0);
        }

        let padded_value_hash = Sha256::digest(&padded_value);
        let tmp_file_name = format!("{}_{}_{}", TMP_PREFIX, key, padded_value_hash);
        let tmp_file_path = self.root_dir.join(&tmp_file_name);

        data_with_hash.extend_from_slice(padded_value_hash.as_slice());
        data_with_hash.extend_from_slice(&padded_value);

        // Now we can save tmp file to the disk
        self.save_data_to_file(&data_with_hash, &tmp_file_path).await;

        let dest_file_path = self.root_dir.join(&key_hash_str);

        self.save_data_to_file(&padded_value, &dest_file_path).await;
        self.remove_file(&tmp_file_path).await;
        self.file_keys.insert(key_hash_str, dest_file_path);

        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>>
    {
        let key_hash = Sha256::digest(key.as_bytes());
        let key_hash_str = format!("{:x}", key_hash);

        if let Some(path) = self.file_keys.get(&key_hash_str)
        {
            return Some(self.read_file_data(path).await);
        }
        None
    }

    async fn remove(&mut self, key: &str) -> bool
    {
        let key_hash = Sha256::digest(key.as_bytes());
        let key_hash_str = format!("{:x}", key_hash);

        if let Some(path) = self.file_keys.get(&key_hash_str)
        {
            // So that we dont have problem we mutable immutable ref
            let path_clone = path.clone();

            // We need to remember to also remove keys from map
            self.file_keys.remove(&key_hash_str);
            self.remove_file(&path_clone).await;

            return true;
        }

        // If no such key present we return false
        false
    }
}

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(
    root_storage_dir: PathBuf
) -> Box<dyn StableStorage> 
{
    // When building new stable storage or recovering, we firstly read all files
    // names so that we can quickly check if given key is present. 
    // We also store tmp_key files, so that we can start recovering and either
    // deleting temp files or creating final files from temp files
    let file_map = match scan_dir_and_create_file_name_file_path_map(&root_storage_dir).await
    {
        Ok(m) => m,
        Err(e) => panic!("build_stable_storage - find_file_paths_and_names - got filesystem operation fails error: '{}'", e)
    };

    let mut storage = Storage::new(file_map, root_storage_dir.clone());
    storage.recover().await;

    Box::new(storage)
}


/// Function scans directory and inserts all file_name : file_path to hashmap
async fn scan_dir_and_create_file_name_file_path_map(
    dir_path: &PathBuf
) -> std::io::Result<HashMap<String, PathBuf>> 
{
    let mut map: HashMap<String, PathBuf> = HashMap::new();

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
                if file_name.len() > KEY_MAX_SIZE
                {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidFilename, 
                        format!("When recovering filesystem we read filename: {} that is too long: {} bytes", file_name, file_name.len())));
                }
                map.insert(file_name.to_string(), entry_path);
            }
        }
    }

    Ok(map)
}