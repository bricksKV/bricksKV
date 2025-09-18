use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::{fs, io};

/// Meta struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Meta {
    pub current_wal_id: u64,
    pub key_size: u32,
}

impl Meta {
    /// Load Meta from file
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let content = fs::read_to_string(path)?;
        let meta: Meta = serde_json::from_str(&content)?;
        Ok(meta)
    }

    /// Save Meta to file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        let mut file = fs::File::create(path)?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Update wal_id and save
    pub fn update_wal_id<P: AsRef<Path>>(&mut self, wal_id: u64, path: P) -> io::Result<()> {
        self.current_wal_id = wal_id;
        self.save_to_file(path)
    }
}