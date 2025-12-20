use tokio::fs as t_fs;
use core::panic;
use std::collections::{HashMap, BTreeSet};
use std::path::PathBuf;
use tokio::sync::MutexGuard;
use tokio::sync::{Semaphore, Mutex};

use crate::domain::{SectorIdx};
use crate::storage::storage_defs::{TimesUsed};

pub struct FileDescriptorManager
{
    descr_semaphore: Semaphore,
    descr_collections: Mutex<DescrCollections>,
}

impl FileDescriptorManager
{
    pub fn new(max_allowed_nbr_of_open_descr: usize) -> FileDescriptorManager
    {
        return FileDescriptorManager { 
            descr_semaphore: Semaphore::new(max_allowed_nbr_of_open_descr),
            descr_collections: Mutex::new(
                DescrCollections::new(max_allowed_nbr_of_open_descr)
            ),
        };
    }

    pub async fn give_back_file_descr(&self, sector_idx: SectorIdx, f: t_fs::File)
    {
        let mut collections_lock = self.descr_collections.lock().await;

        collections_lock.give_back_file_descr(sector_idx, f);

        if collections_lock.is_anyone_waiting_for_descr()
        {
            collections_lock.decrement_waiting_sectors();
            collections_lock.remove_least_used_descritptor();
            // We give one permit, even though number of opened file descriptors is
            // at maximum, however, we know, that at least one of these descriptors
            // is currently not being used
            self.descr_semaphore.add_permits(1);
        }
        // collections_lock drop
    }

    pub async fn take_file_descr(
        &self, 
        sector_idx: SectorIdx,
        sector_path: &PathBuf
    ) -> t_fs::File
    {
        let mut collections_lock = self.descr_collections.lock().await;

        // 0) We check if we have already opened descriptor for given sector
        match collections_lock.try_retrieve_file_descr(sector_idx)
        {
            Some(f) => {
                return f;
            },
            None => {
                // 1) We don't have open file descr in descr_map for this sector, 
                // so check if we can create a new one, by trying to acquire 
                // semaphore that counts how many descriptors we can open
                match self.descr_semaphore.try_acquire()
                {
                    Ok(_) => {
                        return FileDescriptorManager
                            ::handle_successful_semaphore_acquire(
                                sector_idx, 
                                sector_path, 
                                &mut collections_lock
                            ).await;
                    },
                    Err(_) => {
                        // 2) There are no free file descriptors, so we cannot open
                        // new one, so we check if we can close already opened but 
                        // not used descriptor
                        match collections_lock.try_reclaim_file_descr(sector_idx)
                        {
                            Some(_) => {
                                // There was an unused file_descr so we closed it
                                // and reserved space for our descr, so we can drop
                                // mutex and safely open new file_descr
                                drop(collections_lock);

                                let f = open_or_create_file_descr(sector_path).await;
                                return f;
                            },
                            None => {
                                // 3) There is no available permit and all 
                                // descriptors are being used, we need to increase 
                                // nbr of waiting sectors and wait on semaphore for 
                                // sb to let us in
                                collections_lock.increment_waiting_sectors();

                                drop(collections_lock);

                                self.descr_semaphore.acquire().await.unwrap();

                                // We were woken up, this means there are some UNUSED
                                // file descriptors, so we need to reclaim one of 
                                // them
                                let mut collections_lock = 
                                    self.descr_collections.lock().await;

                                return FileDescriptorManager
                                    ::handle_reserve_after_wait(
                                        sector_idx, 
                                        sector_path, 
                                        &mut collections_lock
                                ).await;
                            }
                        }
                    }
                }

            }
        }
    }

    async fn handle_reserve_after_wait(
        sector_idx: SectorIdx,
        sector_path: &PathBuf,
        collections_lock: &mut MutexGuard<'_, DescrCollections>
    ) -> t_fs::File
    {
        match collections_lock.try_reserve_file_descr(sector_idx)
        {
            Some(_) => {
                // We reserved space for our descriptor so we can safely drop mutex 
                // and open our file 
                drop(collections_lock);

                let f = open_or_create_file_descr(sector_path).await;
                return f;
            },
            None => {
                panic!("FileDescriptorManager::handle_reserve_after_wait - we acquired semaphore, there should be free space to reserve file descr, but there is not");
            }
        }
    }

    async fn handle_successful_semaphore_acquire(
        sector_idx: SectorIdx,
        sector_path: &PathBuf,
        collections_lock: &mut MutexGuard<'_, DescrCollections>
    ) -> t_fs::File
    {
        match collections_lock
            .try_reserve_file_descr(sector_idx)
        {
            Some(_) => {
                // place for our file descr was reserved, and we 
                // acquired semaphore for it so we can safely drop 
                // mutex here 
                drop(collections_lock);

                // and open file
                let f = open_or_create_file_descr(sector_path).await;
                return f;
            },
            None => {
                // This probably should never happen since Semaphores
                // are FAIR, and implement queue, so if semaphore is
                // incremented, first task on queue should acquire it
                // and task doing try_acquire shouldn't be able to do it quicker, 
                // but just to be sure, we handle this case here.
                // So even though we acquired Semaphore, we couldn't 
                // reserve place for new descr, this means that we
                // have been passed a critical section, meaning all
                // descriptors are open, but THERE ARE UNUSED ones
                match collections_lock.try_reclaim_file_descr(sector_idx)
                {
                    Some(_) => {
                        // There was an unused file_descr so we closed it
                        // and reserved space for our descr, so we can drop
                        // mutex and safely open new file_descr
                        drop(collections_lock);

                        let f = open_or_create_file_descr(sector_path).await;
                        return f;
                    },
                    None => {
                        panic!("FileDescriptorManager::handle_successful_sem_acquire - we acquired semaphore, there was no free space to reserve file descr, but there should be unused descriptors, but after try_reclaim we couldn't reclaim unused semaphore");
                    }
                }
            }
        }
    }
}

struct DescrCollections
{
    descr_map: HashMap<SectorIdx, (TimesUsed, Option<tokio::fs::File>)>,
    curr_used_descr: BTreeSet<(TimesUsed, SectorIdx)>,
    curr_not_used_descr: BTreeSet<(TimesUsed, SectorIdx)>,
    max_allowed_nbr_of_open_descr: usize,
    n_sectors_waiting: usize,

}

impl DescrCollections
{
    fn new(max_allowed_nbr_of_open_descr: usize) -> DescrCollections
    {
        return DescrCollections {
            descr_map: HashMap::new(), 
            curr_used_descr: BTreeSet::new(),
            curr_not_used_descr: BTreeSet::new(),
            max_allowed_nbr_of_open_descr,
            n_sectors_waiting: 0,
        };
    }

    fn increment_waiting_sectors(&mut self)
    {
        self.n_sectors_waiting += 1;
    }

    fn decrement_waiting_sectors(&mut self)
    {
        self.n_sectors_waiting.checked_sub(1).expect("DescrCollections::decrement_waiting_sectors - decremented sectors when sectors waiting are 0");
    }

    fn is_anyone_waiting_for_descr(&self) -> bool
    {
        return self.n_sectors_waiting > 0;
    }

    fn give_back_file_descr(&mut self, sector_idx: SectorIdx, f: t_fs::File)
    {
        if let Some((usage_count, f_descr)) = self.descr_map.get_mut(&sector_idx)
        {
            if !self.curr_used_descr.remove(&(*usage_count, sector_idx))
            {
                panic!("DescrCollections::give_back_file_descr: When we wanted to remove (usage_count, sector_idx) = ({}, {}) from curr_used_descr set it was not present inside set, this shouldn't have happened", *usage_count, sector_idx);
            }
            if f_descr.is_some()
            {
                panic!("DescrCollections::give_back_file_descr - f_descr inside descr_map is SOME, but it should be NONE, since now we are setting it to SOME");
            }

            *f_descr = Some(f);

            self.curr_not_used_descr.insert((*usage_count, sector_idx));

            return;
        }
        else
        {
            panic!("DescrCollections::give_back_file_descr - sector: {} is not inside descr_map", sector_idx);
        }
    }

    /// If file_descr is present, function **RETURNS OWNERSHIP** of this file_descr.
    /// In desc_map we leave None as value.
    fn try_retrieve_file_descr(
        &mut self, 
        sector_idx: SectorIdx
    ) -> Option<t_fs::File>
    {
        if let Some((usage_count, f_descr)) = self.descr_map.get_mut(&sector_idx)
        {
            // If we have file descriptor inside map, this means that semaphor for 
            // this descriptor was taken, and we can safely use it, and also that 
            // after ending last operation it was moved from used to free_descr set
            if !self.curr_not_used_descr.remove(&(*usage_count, sector_idx))
            {
                panic!("DescrCollections::get_file_descr: When we wanter to remove (usage_count, sector_idx) = ({}, {}) from curr_free_descr set it was not present inside set, this shouldn't have happened", *usage_count, sector_idx);
            }

            // in f_descr we leave None
            let owned_f = f_descr.take();

            match owned_f  
            {
                Some(f) => {
                    *usage_count += 1;

                    self.curr_used_descr.insert((*usage_count, sector_idx));

                    return Some(f);
                }
                None => {
                    // After ending operation on file descriptor we should always
                    // return it to the descr_map, if this didn't happen we are in
                    // corrupted state
                    panic!("DescrCollections::get_file_descr: for sector_idx({}) file descriptor is NONE, this shouldn't have happened", sector_idx);
                }
            };
        }
        return None;
    }

    /// If nbr of all opened descr is less than maximum allowed, we reserve space for
    /// new descriptor, **which will be opened OUTSIDE of this function**, since 
    /// DescrCollections is inside mutex, and opening file here would be a huge 
    /// bottleneck
    fn try_reserve_file_descr(
        &mut self, 
        sector_idx: SectorIdx
    ) -> Option<()>
    {
        if self.curr_not_used_descr.len() 
        + self.curr_used_descr.len() < self.max_allowed_nbr_of_open_descr
        {
            let usage_count: u64 = 0;
            self.descr_map.insert(sector_idx, (usage_count, None));
            self.curr_used_descr.insert((usage_count, sector_idx));

            return Some(());
        }
        return None;
    }

    /// Function removes currently not used file descriptor, which was least used,
    /// and in its place opens new file descriptor for given sector_idx.
    /// If there are no unused descriptors returns None.
    fn try_reclaim_file_descr(
        &mut self, 
        sector_idx: SectorIdx
    ) -> Option<()>
    {
        match self.remove_least_used_descritptor()
        {
            Some(_) => {
                return self.try_reserve_file_descr(sector_idx);
            },
            None => {
                // All opened file descriptors are currently used, so we cannot open 
                // a new one
                return None;

            }
        }
    }

    fn remove_least_used_descritptor(&mut self) -> Option<()>
    {
        let least_used_descr = self.curr_not_used_descr.first();

        if let Some(&(use_count, least_used_sector_idx)) = least_used_descr 
        {
            self.curr_not_used_descr.remove(&(use_count, least_used_sector_idx));

            let (_, old_f_descr) = self.descr_map
                .remove(&least_used_sector_idx)
                .expect(&format!("DescrCollections::try_reclaim_file_descr:: descriptor for sector '{}' was present in curr_not_used_descr set but not in descr_map", least_used_sector_idx));

            // We explicitly close file descriptor before allowing to open a new one
            drop(old_f_descr.expect(
                &format!("DescrCollections::try_reclaim_file_descr - when trying to close descriptor for sector: '{}', it was NONE, but should be SOME", least_used_sector_idx)
            ));
            return Some(());
        }
        return None;
    }
}

async fn open_or_create_file_descr(sector_path: &PathBuf) -> t_fs::File
{
    let f = t_fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(sector_path)
        .await
        .expect(&format!("open_or_create_file: failed for {:?}", sector_path));
    return f;
}