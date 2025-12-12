use std::{path::PathBuf};
use tokio::fs as t_fs;
use std::collections::{HashMap};
use sha2::{Sha256, Digest};
use std::io::ErrorKind;
// You can add here other imports from std or crates listed in Cargo.toml.

// You can add any private types, structs, consts, functions, methods, etc., you need.
// As always, you should not modify the public interfaces.
const BUF_SIZE: usize = 65535;
const KEY_MAX_SIZE: usize = 255;
const HASH_SIZE: usize = 32;
const TMP_PREFIX: &str = "tmp_";

struct Storage
{
    root_dir: PathBuf,
    file_keys: HashMap<String, PathBuf>,
}

impl Storage
{
    pub fn new(keys: HashMap<String, PathBuf>, dst_dir: PathBuf) -> Storage
    {
        Storage{
            root_dir: dst_dir,
            file_keys: keys,
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

        let data = t_fs::read(tmp_file_path).await.expect("check_tmp_file - tokio::fs::read failed");

        // We need to check if data is at most BUF_SIZE + HASH_SIZE, if our 
        // programme runs correctly this will always be true, but if sb tempered
        // with our storage this might happen
        if data.len() > BUF_SIZE + HASH_SIZE
        {
            panic!("check_tmp_file - File: '{tmp_file_name}' is too large, this shouldn't have happened");
        }

        // Hash plus at least one byte of data
        if data.len() < HASH_SIZE + 1
        {
            panic!("check_tmp_file - File: '{tmp_file_name}' is too small, this shouldn't have happened");
        }

        // Now we can safely extract hash and file data 
        let (file_hash, file_data) = data.split_at(HASH_SIZE);
        let computed_hash = Sha256::digest(file_data);

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

        // read_file_data is currently used only for reading NOT TEMP FILES,
        // thus this check is alright, however if we wanted to use it for TEMP
        // FILES also, their size might be at most BUF_SIZE + HASH_SIZE
        if data.len() > BUF_SIZE || data.is_empty() 
        {
            panic!("read_file_data - read data len is greater than BUF_SIZE or is empty - this shouldn't have happened");
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
        if value.len() > BUF_SIZE
        {
            return Err(format!("Provided value: '{}' bytes is longer than allowed {} bytes", value.len(), BUF_SIZE));
        }
        if value.is_empty()
        {
            return Err(format!("Provided value has '{}' bytes, we do not allow to store empty value ", value.len()));
        }

        // key can be anything, thus can also contain characters not allowed as
        // file name, therefore we firstly hash key, and will use this hash to 
        // create a file name with always fixed size and correct characters 
        let key_hash = Sha256::digest(key.as_bytes());
        let key_hash_str = format!("{:x}", key_hash);

        let tmp_file_name = format!("{}{}", TMP_PREFIX, key_hash_str);
        let tmp_file_path = self.root_dir.join(&tmp_file_name);

        let value_hash = Sha256::digest(value);
        let mut data_with_hash: Vec<u8> = Vec::with_capacity(HASH_SIZE + value.len());

        data_with_hash.extend_from_slice(value_hash.as_slice());
        data_with_hash.extend_from_slice(value);

        // Now we can save tmp file to the disk
        self.save_data_to_file(&data_with_hash, &tmp_file_path).await;

        let dest_file_path = self.root_dir.join(&key_hash_str);

        self.save_data_to_file(value, &dest_file_path).await;
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
    let file_map = match find_file_paths_and_names(&root_storage_dir).await
    {
        Ok(m) => m,
        Err(e) => panic!("build_stable_storage - find_file_paths_and_names - got filesystem operation fails error: '{}'", e)
    };

    let mut storage = Storage::new(file_map, root_storage_dir.clone());
    storage.recover().await;

    Box::new(storage)
}


async fn find_file_paths_and_names(
    path: &PathBuf
) -> std::io::Result<HashMap<String, PathBuf>> 
{
    let mut map: HashMap<String, PathBuf> = HashMap::new();

    // we panic when filesystem function fails
    let mut entries = t_fs::read_dir(path).await.unwrap();

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