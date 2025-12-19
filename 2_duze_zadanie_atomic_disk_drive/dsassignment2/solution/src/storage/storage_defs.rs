use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::domain::{SectorIdx};

/// While doing WRITE we have open descriptor for both old file and tmp file,
/// so if we get more than 500 WRITE requests, we would have more than 1024 open 
/// descriptors which would crash our programme.
/// 2 * 491 is 982, so in worst scenario we have 42 free descriptors.
/// We need them since at the same time we can have 16 TCP connections, each takes 
/// one descriptor, and some OS reserved descriptors etc
pub const MAX_AVAILABLE_FILE_DESCRIPTORS: usize = 980;
// pub const CHECKSUM_SIZE: usize = 32;
pub const TMP_PREFIX: &str = "tmp";

pub type TimesRead = u64;
pub type TimeStampType = u64;
pub type WriterRankType = u8;
pub type SectorRwHashMap = RwLock<HashMap<SectorIdx, (TimeStampType, WriterRankType)>>;