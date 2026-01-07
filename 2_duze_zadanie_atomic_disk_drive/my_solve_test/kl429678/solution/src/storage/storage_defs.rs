use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::domain::{SectorIdx};

/// We can have at most 255 open TCP descriptors for ATOMIC DRIVE PROCESSES plus 
/// reserved descriptors like STDIN plus 16 clients making requests, but each client
/// can make many requests
/// So 700 is a safe number that leaves a margin 
pub const MAX_AVAILABLE_FILE_DESCRIPTORS: usize = 700;
pub const TMP_PREFIX: &str = "tmp";

pub type TimesUsed = u64;
pub type TimeStampType = u64;
pub type WriterRankType = u8;
pub type SectorRwHashMap = RwLock<HashMap<SectorIdx, (TimeStampType, WriterRankType)>>;