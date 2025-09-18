use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub struct WALOptions {
    fsync: bool,
}

/// Simplified WAL: sequential write + partially concurrent write
pub struct WAL {
    file: File,
    end_offset: u64,
    fsync: bool,
}

impl Default for WALOptions {
    fn default() -> Self {
        WALOptions { fsync: true }
    }
}

impl WAL {
    /// Open a WAL file
    pub fn open(path: &Path, fsync: bool) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let end_offset = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            end_offset,
            fsync,
        })
    }

    /// Sequentially write a record (maintains mutable reference)
    pub fn write_record(&mut self, payload: Vec<u8>) -> io::Result<u64> {
        let length = payload.len() as u32;
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&payload);

        let offset = self.end_offset;
        self.file.write_at(&buf, offset)?;
        if self.fsync {
            self.file.sync_all()?;
        }
        self.end_offset += buf.len() as u64;
        Ok(offset + buf.len() as u64)
    }

    /// Sequentially read WAL and replay
    pub fn replay<F>(&self, mut callback: F) -> io::Result<()>
    where
        F: FnMut(Vec<u8>),
    {
        let file_len = self.file.metadata()?.len();
        let mut offset = 0;

        while offset < file_len {
            // Read the length
            let mut len_bytes = [0u8; 4];

            // If remaining bytes are less than 4, the file tail is corrupted; truncate and exit
            if offset + 4 > file_len {
                break;
            }

            self.file.read_at(&mut len_bytes, offset)?;
            let length = u32::from_le_bytes(len_bytes) as u64;
            offset += 4;

            // If remaining bytes are less than length, the file tail is corrupted; truncate and exit
            if offset + length > file_len {
                break;
            }
            let mut payload = vec![0u8; length as usize];
            self.file.read_at(&mut payload, offset)?;
            offset += length;
            callback(payload);
        }

        Ok(())
    }
}

const WAL_FILE_SUFFIX: &str = ".wal";

/// Generate WAL file path
pub fn wal_file_path(base_dir: &Path, wal_id: u64) -> PathBuf {
    base_dir.join(format!("{}{}", wal_id, WAL_FILE_SUFFIX))
}

/// Get all WAL IDs in a directory
pub fn get_all_wal_ids<P: AsRef<Path>>(path: P) -> Vec<u64> {
    let mut ids = Vec::new();
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                // Assume filename format is <u64>.wal
                if let Some(id_str) = file_name.strip_suffix(WAL_FILE_SUFFIX) {
                    if let Ok(id) = id_str.parse::<u64>() {
                        ids.push(id);
                    }
                }
            }
        }
    }
    ids
}