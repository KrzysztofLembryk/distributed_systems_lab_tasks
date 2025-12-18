use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::domain::{SectorIdx};

// pub const CHECKSUM_SIZE: usize = 32;
pub const TMP_PREFIX: &str = "tmp";

pub type TimeStampType = u64;
pub type WriterRankType = u8;
pub type SectorRwHashMap = RwLock<HashMap<SectorIdx, (TimeStampType, WriterRankType)>>;