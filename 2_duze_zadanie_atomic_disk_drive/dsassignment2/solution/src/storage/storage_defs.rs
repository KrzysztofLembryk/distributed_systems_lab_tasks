use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::domain::{SectorIdx};

/// We can have at most 255 open TCP descriptors plus reserved descriptors like STDIN
/// So 745 is a safe number that leaves a few free, unused descriptors
pub const MAX_AVAILABLE_FILE_DESCRIPTORS: usize = 745;
// pub const CHECKSUM_SIZE: usize = 32;
pub const TMP_PREFIX: &str = "tmp";

pub type TimesUsed = u64;
pub type TimeStampType = u64;
pub type WriterRankType = u8;
pub type SectorRwHashMap = RwLock<HashMap<SectorIdx, (TimeStampType, WriterRankType)>>;