use std::{path::PathBuf};
use tokio::fs as t_fs;
use std::collections::{HashMap};
use sha2::{Sha256, Digest};
use std::io::ErrorKind;
use crate::domain::{SECTOR_SIZE, SectorIdx, SectorVec};
use crate::sectors_manager_public::{SectorsManager};
use crate::storage::storage_defs::{TMP_PREFIX};
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
struct Storage
{
    root_dir: PathBuf,
    file_name_to_path_map: HashMap<String, PathBuf>,
    sector_idx_to_file_name_map: HashMap<SectorIdx, String>,
}

impl Storage
{
    pub fn new(
        root_dir: PathBuf,
        file_name_to_path_map: HashMap<String, PathBuf>, 
    ) -> Storage
    {
        Storage{
            root_dir,
            file_name_to_path_map,
            sector_idx_to_file_name_map: HashMap::new(),
        }
    }

    pub async fn recover(&mut self)
    {
        // we clone here so that we can modify our map inside check_tmp_file
        let keys = self.file_name_to_path_map.clone();

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

        // After above loop all tmp files are deleted and we have only data that was
        // not corrupted, and only file_names in our map, so we can create sector_idx
        // to file_name map
        for (file_name, _) in &self.file_name_to_path_map
        {
            let (sector_idx, _, _) = extract_data_from_file_name(file_name);
            self.sector_idx_to_file_name_map
                .insert(sector_idx, file_name.to_string());
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
            panic!("check_tmp_file was given a file name: '{tmp_file_name}' without 'tmp' prefix, this might cause corruption");
        }

        let (sector_idx, timestamp, writer_rank, checksum) = extract_data_from_temp_file_name(&tmp_file_name);

        let data = t_fs::read(tmp_file_path).await.expect("check_tmp_file - tokio::fs::read failed");

        // if our programme runs correctly this will always be true, since we do ONE 
        // FILE FOR ONE SECTOR, but if sb tempered with our storage this might happen
        if data.len() != SECTOR_SIZE 
        {
            panic!("check_tmp_file - File: '{tmp_file_name}' size ({}) is not equal to SECTOR_SIZE ({}), this shouldn't have happened", data.len(), SECTOR_SIZE);
        }

        let computed_hash = Sha256::digest(&data);

        if computed_hash.as_slice() != checksum.as_bytes()
        {
            // We will remove tmp file since it was corrupted
            self.remove_file(tmp_file_path).await;
            self.file_name_to_path_map.remove(tmp_file_name);

            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Checksum mismatch"
            )); 
        }

        // computed_hash is correct so we need to overwrite dstfile
        // If the checksum is correct, tmpfile was fully written and a crash happened later. In this case the write can be resumed using the data from tmpfile
        let dest_file_name = create_file_name(sector_idx, timestamp, writer_rank);
        let dest_path = self.root_dir.join(&dest_file_name);

        self.save_data_to_file(&data, &dest_path).await;
        self.remove_file(tmp_file_path).await;

        self.file_name_to_path_map.remove(tmp_file_name);
        self.file_name_to_path_map.insert(dest_file_name, dest_path.clone());

        Ok(())
    }

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

    async fn remove_file(&self, path: &PathBuf)
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


/// Creates a new instance of stable storage.
async fn build_stable_storage(
    root_storage_dir: PathBuf,
) -> Box<Storage>  //-> Box<dyn SectorsManager> 
{
    // When building new stable storage or recovering, we firstly read all files
    // names so that we can quickly check if we have temp files and either recover 
    // them or remove them. 
    let file_map = match scan_dir_and_create_file_name_to_path_map(&root_storage_dir).await
    {
        Ok(m) => m,
        Err(e) => panic!("build_stable_storage - find_file_paths_and_names - got filesystem operation fails error: '{}'", e)
    };

    let mut storage = Storage::new(root_storage_dir, file_map);
    storage.recover().await;

    Box::new(storage)
}


