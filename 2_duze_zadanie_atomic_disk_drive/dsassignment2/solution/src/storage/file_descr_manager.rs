use std::collections::{HashMap, HashSet};
use tokio::sync::{Semaphore, Mutex};

use crate::domain::{SectorIdx};
use crate::storage::storage_defs::{TimesRead};

pub struct FileDescriptorManager
{
    max_allowed_nbr_of_open_descr: usize,
    descr_semaphore: Semaphore,
    descr_collections: Mutex<DescrCollections>,
}

impl FileDescriptorManager
{
    pub fn new(max_allowed_nbr_of_open_descr: usize) -> FileDescriptorManager
    {
        return FileDescriptorManager { 
            max_allowed_nbr_of_open_descr, 
            descr_semaphore: Semaphore::new(max_allowed_nbr_of_open_descr),
            descr_collections: Mutex::new(DescrCollections::new()),
        };
    }

    pub fn try_get_file_descr(sector_idx: SectorIdx)
    {

    }
}

struct DescrCollections
{
    descr_map: HashMap<SectorIdx, tokio::fs::File>,
    curr_used_descr: HashSet<(TimesRead, SectorIdx)>,
    curr_free_descr: HashSet<(TimesRead, SectorIdx)>,
}

impl DescrCollections
{
    fn new() -> DescrCollections
    {

        return DescrCollections {
            descr_map: HashMap::new(), 
            curr_used_descr: HashSet::new(),
            curr_free_descr:  HashSet::new(),
        };
    }
}