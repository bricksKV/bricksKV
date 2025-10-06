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
            .append(true) // O_APPEND
            .read(true)
            .open(path)?;
        let end_offset = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            end_offset,
            fsync,
        })
    }
    
    pub fn flush(&mut self) -> io::Result<()> {
        if self.fsync {
            self.file.sync_all()?;
        }
        Ok(())
    }

    /// Sequentially write a record (maintains mutable reference)
    pub fn write_record(&mut self, payload: Vec<u8>) -> io::Result<u64> {
        let payload = compress_data(&payload);
        let length = payload.len() as u32;
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&payload);

        let offset = self.end_offset;
        self.file.write(&buf)?;
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
        if file_len == 0 {
            return Ok(());
        }

        let mut offset = 0;
        let mut buf = vec![0u8; file_len as usize];

        // 一次性读入整个文件
        self.file.read_exact_at(&mut buf, 0)?;

        while offset + 4 <= buf.len() {
            let length = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if offset + length > buf.len() {
                break; // 文件尾部损坏
            }

            let payload = &buf[offset..offset + length];
            let payload = de_compress_data(payload);
            callback(payload);

            offset += length;
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

fn compress_data(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 3).unwrap()
}

fn de_compress_data(data: &[u8]) -> Vec<u8> {
    zstd::decode_all(data).unwrap()
}