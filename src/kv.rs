mod data;
mod index;
mod meta;
mod utils;
mod wal;

use crate::kv::data::level_page_bitmap::LevelPageOptions;
use crate::kv::index::buckets::BucketsOptions;
use crate::kv::meta::Meta;
use crate::kv::utils::{path_exist, remove_file_if_exists};
use crate::kv::wal::{WAL, get_all_wal_ids, wal_file_path};
use data::level_page_bitmap;
use index::bucket::BucketValue;
use index::buckets::{Buckets, BucketsError};
use log::error;
use std::collections::HashMap;
use std::fs::{create_dir, create_dir_all};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::sleep;
use std::time::Duration;
use std::{error, fs, io, thread};

struct FlushingBuffer {
    buffer: HashMap<Vec<u8>, KVOp>,
    wal_path: PathBuf,
}

pub struct KV {
    dir: PathBuf,
    meta: RwLock<Meta>,
    level_page_bitmap: Arc<level_page_bitmap::LevelPage>,
    buckets_index: Arc<Buckets<DataInfo>>,
    key_size: u32,
    current_wal: RwLock<WAL>,
    current_wal_id: AtomicU64,
    current_buffer: RwLock<HashMap<Vec<u8>, KVOp>>,
    flushing_buffers: Arc<RwLock<Vec<FlushingBuffer>>>,
    flush_lock: Arc<Mutex<()>>,
    wal_flush_size: u32,
}

#[derive(Clone, Debug)]
struct DataInfo {
    data_id: u64,
    data_len: u32,
}

impl BucketValue for DataInfo {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4);
        buf.extend(&self.data_id.to_le_bytes());
        buf.extend(&self.data_len.to_le_bytes());
        buf
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 12 {
            return None;
        }
        let data_id = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let data_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        Some(DataInfo { data_id, data_len })
    }
}

/// General KV error, contains only IO and Other
#[derive(Debug)]
pub enum KVError {
    Io(io::Error),
    InvalidKeyLength,
    Other(String),
}

/// Conversion from BucketsError
impl From<BucketsError> for KVError {
    fn from(err: BucketsError) -> Self {
        match err {
            BucketsError::Io(e) => KVError::Io(e),
            BucketsError::InvalidKeyLength => KVError::InvalidKeyLength,
            BucketsError::MaxSearchReached => KVError::Other("Max search reached".to_string()),
            BucketsError::Other(s) => KVError::Other(s),
        }
    }
}

/// Allow direct conversion from io::Error
impl From<io::Error> for KVError {
    fn from(err: io::Error) -> Self {
        KVError::Io(err)
    }
}

/// Display implementation
impl std::fmt::Display for KVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KVError::Io(e) => write!(f, "IO error: {}", e),
            KVError::Other(s) => write!(f, "Other error: {}", s),
            &KVError::InvalidKeyLength => write!(f, "InvalidKeyLength error"),
        }
    }
}

impl error::Error for KVError {}

const BUCKETS_INDEX_DIR_NAME: &str = "buckets-index";

const WAL_DIR_NAME: &str = "wal";

const KV_META_FILE_NAME: &str = "kv.meta";

pub struct WALOptions {
    pub flush_size: u32,
    pub fsync: bool,
}

impl Default for WALOptions {
    fn default() -> Self {
        WALOptions {
            flush_size: 4 * 1024 * 1024,
            fsync: true,
        }
    }
}

#[derive(Default)]
pub struct KVOptions {
    pub key_store_options: BucketsOptions,
    pub data_store_options: LevelPageOptions,
    pub wal_options: WALOptions,
}

/// Represents a single KV operation: Put or Delete
pub enum KVOp {
    Put { value: Vec<u8> },
    Del {},
}

pub struct Batch {
    ops: Vec<(Vec<u8>, KVOp)>,
}

impl KV {
    /// Initialize KV storage: provide storage directory and page_size sequence
    pub fn new<P: Into<PathBuf>>(dir: P, opts: KVOptions) -> Result<Self, KVError> {
        let dir = dir.into();
        create_dir_all(&dir)?;

        let level_page_bitmap = Arc::new(level_page_bitmap::LevelPage::new(
            &dir, // Each page_size file under the directory
            LevelPageOptions::default(),
        )?);

        let bucket_index = Arc::new(Buckets::new(
            dir.join(BUCKETS_INDEX_DIR_NAME),
            BucketsOptions::default(),
        )?);

        let kv_meta_file_path = dir.join(KV_META_FILE_NAME);
        let mut kv_meta = Meta {
            current_wal_id: 0,
            key_size: opts.key_store_options.key_size,
        };
        let mut need_load_data = false;
        let mut current_wal_path = wal_file_path(dir.to_path_buf().join(WAL_DIR_NAME).as_path(), 0);
        if path_exist(&kv_meta_file_path)? {
            kv_meta = Meta::load_from_file(&kv_meta_file_path)?;
            current_wal_path = wal_file_path(
                dir.to_path_buf().join(WAL_DIR_NAME).as_path(),
                kv_meta.current_wal_id,
            );
            need_load_data = true;
        } else {
            create_dir_all(dir.to_path_buf().join(WAL_DIR_NAME))?;
            kv_meta.save_to_file(&kv_meta_file_path)?;
        }
        let current_wal = RwLock::new(WAL::open(
            current_wal_path.as_path(),
            opts.wal_options.fsync,
        )?);
        let current_wal_id = AtomicU64::new(kv_meta.current_wal_id);
        let kv = Self {
            dir: dir.to_path_buf(),
            meta: RwLock::from(kv_meta),
            level_page_bitmap,
            buckets_index: bucket_index,
            key_size: opts.key_store_options.key_size,
            current_wal,
            current_wal_id,
            current_buffer: Default::default(),
            flushing_buffers: Arc::new(Default::default()),
            flush_lock: Arc::new(Mutex::new(())),
            wal_flush_size: opts.wal_options.flush_size,
        };
        if need_load_data {
            kv.load()?;
        }
        Ok(kv)
    }

    pub fn load(&self) -> Result<(), KVError> {
        let mut wal_ids = get_all_wal_ids(self.dir.to_path_buf().join(WAL_DIR_NAME));
        for id in &wal_ids {
            if *id > self.meta.read().unwrap().current_wal_id {
                let wal_file_path = self.wal_file_path(*id);
                remove_file_if_exists(wal_file_path.as_path())?;
            }
        }
        wal_ids.retain(|&id| id <= self.meta.read().unwrap().current_wal_id);
        let key_size = self.key_size as usize;
        for wal_id in wal_ids {
            let wal_file_path = self.wal_file_path(wal_id);
            let wal = WAL::open(wal_file_path.as_path(), true)?;
            if wal_id == self.meta.read().unwrap().current_wal_id {
                let mut current_buffer = self.current_buffer.write().unwrap();
                wal.replay(|batch_payload| {
                    let mut batch_offset = 0;
                    while batch_offset < batch_payload.len() {
                        let entry_len = u32::from_le_bytes(
                            batch_payload[batch_offset..batch_offset + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        batch_offset += 4;
                        let entry_payload = &batch_payload[batch_offset..batch_offset + entry_len];
                        batch_offset += entry_len;

                        if entry_len > key_size {
                            // Put operation
                            let key = entry_payload[..key_size].to_vec();
                            let value = entry_payload[key_size..].to_vec();
                            current_buffer.insert(key, KVOp::Put { value });
                        } else {
                            // Delete operation
                            let key = entry_payload[..key_size].to_vec();
                            current_buffer.insert(key, KVOp::Del {});
                        }
                    }
                })?;
            } else {
                let mut buffer: HashMap<Vec<u8>, KVOp> = HashMap::new();
                // replay, split each record by key_size
                wal.replay(|batch_payload| {
                    let mut batch_offset = 0;
                    while batch_offset < batch_payload.len() {
                        let entry_len = u32::from_le_bytes(
                            batch_payload[batch_offset..batch_offset + 4]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        batch_offset += 4;
                        let entry_payload = &batch_payload[batch_offset..batch_offset + entry_len];
                        batch_offset += entry_len;

                        if entry_len > key_size {
                            // Put operation
                            let key = entry_payload[..key_size].to_vec();
                            let value = entry_payload[key_size..].to_vec();
                            buffer.insert(key, KVOp::Put { value });
                        } else {
                            // Delete operation
                            let key = entry_payload[..key_size].to_vec();
                            buffer.insert(key, KVOp::Del {});
                        }
                    }
                })?;
                self.flushing_buffers.write().unwrap().push(FlushingBuffer {
                    buffer,
                    wal_path: wal_file_path,
                })
            }
        }
        Ok(())
    }

    /// Single put operation
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KVError> {
        // Wrap into a single-op batch and call do_batch
        let batch = Batch {
            ops: vec![(key, KVOp::Put { value })],
        };
        self.do_batch(batch)
    }

    /// Single delete operation
    pub fn delete(&self, key: Vec<u8>) -> Result<(), KVError> {
        // Wrap into a single-op batch and call do_batch
        let batch = Batch {
            ops: vec![(key, KVOp::Del {})],
        };
        self.do_batch(batch)
    }

    /// Batch put/delete
    pub fn do_batch(&self, batch: Batch) -> Result<(), KVError> {
        let mut wal_with_write_lock = self.current_wal.write().unwrap();
        let mut flush_buffer = false;
        let mut pre_wal_path = None;

        // Compute total payload size
        // Total payload length: each entry has 4 bytes representing entry length + entry data
        let total_size: usize = batch
            .ops
            .iter()
            .map(|(key, op)| {
                return match op {
                    KVOp::Put { value } => key.len() + value.len(),
                    KVOp::Del {} => key.len(),
                } + 4; // entry_len
            })
            .sum();

        // Payload = [total_size 4 bytes] + data
        let mut payload = Vec::with_capacity(total_size + 4);
        payload.extend_from_slice(&(total_size as u32).to_le_bytes());

        // Append each op to payload
        for (key, op) in &batch.ops {
            match op {
                KVOp::Put { value } => {
                    if key.len() != self.key_size as usize {
                        return Err(KVError::InvalidKeyLength);
                    }
                    let entry_len = (key.len() + value.len()) as u32;
                    payload.extend_from_slice(&entry_len.to_le_bytes()); // total length
                    payload.extend_from_slice(key);
                    payload.extend_from_slice(value);
                }
                KVOp::Del {} => {
                    if key.len() != self.key_size as usize {
                        return Err(KVError::InvalidKeyLength);
                    }
                    payload.extend_from_slice(&self.key_size.to_le_bytes()); // key length only
                    payload.extend_from_slice(key);
                }
            }
        }

        // Write to WAL
        let size = wal_with_write_lock.write_record(payload)?;
        if size > self.wal_flush_size as u64 {
            pre_wal_path = Some(self.wal_file_path(self.current_wal_id.load(Ordering::Relaxed)));
            let next_wal_id = self.current_wal_id.load(Ordering::Relaxed) + 1;
            let next_wal_path = self.wal_file_path(next_wal_id);
            *wal_with_write_lock = WAL::open(next_wal_path.as_path(), true)?;
            self.current_wal_id.fetch_add(1, Ordering::Relaxed);
            flush_buffer = true;
        }

        // Update in-memory buffer
        {
            let mut buffer_with_write_lock = self.current_buffer.write().unwrap();
            buffer_with_write_lock.extend(batch.ops);

            if flush_buffer {
                let pre_buffer =
                    std::mem::replace(&mut *buffer_with_write_lock, Default::default());
                self.flushing_buffers.write().unwrap().push(FlushingBuffer {
                    buffer: pre_buffer,
                    wal_path: pre_wal_path.unwrap(),
                });
                self.trigger_async_flush();
            }
        }

        Ok(())
    }

    fn wal_file_path(&self, wal_id: u64) -> PathBuf {
        wal_file_path(self.dir.join(WAL_DIR_NAME).as_path(), wal_id)
    }

    pub fn trigger_async_flush(&self) {
        {
            if !self.flush_lock.try_lock().is_ok() {
                return;
            }
        }

        let flushing_buffers = self.flushing_buffers.clone();
        let flush_lock = self.flush_lock.clone();
        let level_page_bitmap = self.level_page_bitmap.clone();
        let buckets_index = self.buckets_index.clone();

        thread::spawn(move || {
            let _guard = flush_lock.lock().unwrap();
            loop {
                {
                    let mut flushing_buffers_with_read_lock = flushing_buffers.read().unwrap();
                    if flushing_buffers_with_read_lock.is_empty() {
                        return;
                    }
                    if let Some(flushing_buffer) = flushing_buffers_with_read_lock.get(0) {
                        for (key, op) in &flushing_buffer.buffer {
                            match op {
                                KVOp::Put { value } => {
                                    let value_len = value.len();
                                    let data_id = loop {
                                        match level_page_bitmap.write(value.clone()) {
                                            Ok(id) => break id,
                                            Err(e) => {
                                                error!(
                                                    "Failed to write level_page_bitmap, retrying: {:?}",
                                                    e
                                                );
                                                sleep(Duration::from_secs(1));
                                            }
                                        }
                                    };
                                    let data_info = DataInfo {
                                        data_id,
                                        data_len: value_len as u32,
                                    };
                                    loop {
                                        match buckets_index.put(key.clone(), data_info.clone()) {
                                            Ok(_) => break,
                                            Err(e) => {
                                                error!(
                                                    "Failed to put into buckets_index, retrying: {:?}",
                                                    e
                                                );
                                                sleep(Duration::from_secs(1));
                                            }
                                        }
                                    }
                                }
                                KVOp::Del { .. } => loop {
                                    match buckets_index.del(key) {
                                        Ok(data_info) => {
                                            if let Some(data_info) = data_info {
                                                let data_id = data_info.data_id;
                                                loop {
                                                    match level_page_bitmap.free(data_id) {
                                                        Ok(_) => break,
                                                        Err(err) => {
                                                            error!("free data_id error: {:?}", err);
                                                            sleep(Duration::from_secs(1));
                                                            continue;
                                                        }
                                                    }
                                                }
                                            }
                                            break;
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to del key from buckets_index, retrying: {:?}",
                                                e
                                            );
                                            sleep(Duration::from_secs(5));
                                            continue;
                                        }
                                    }
                                },
                            }
                        }

                        loop {
                            match remove_file_if_exists(flushing_buffer.wal_path.clone()) {
                                Ok(_) => break,
                                Err(e) => {
                                    error!("Failed to remove WAL file, retrying: {:?}", e);
                                    sleep(Duration::from_secs(5));
                                }
                            }
                        }
                    }
                }
                let mut flushing_buffers_with_write_lock = flushing_buffers.write().unwrap();
                flushing_buffers_with_write_lock.remove(0);
            }
        });
    }

    /// Read key-value
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KVError> {
        if let Some(op) = self.current_buffer.read().unwrap().get(key) {
            match op {
                KVOp::Put { value } => return Ok(Some(value.clone())),
                KVOp::Del { .. } => {
                    return Ok(None);
                }
            }
        }

        let flushing_buffers_with_read_lock = self.flushing_buffers.read().unwrap();
        for flushing_buffer in flushing_buffers_with_read_lock.iter() {
            if let Some(op) = flushing_buffer.buffer.get(key) {
                match op {
                    KVOp::Put { value } => return Ok(Some(value.clone())),
                    KVOp::Del { .. } => {
                        return Ok(None);
                    }
                }
            }
        }

        if let Some(data_info) = self.buckets_index.get(key)? {
            // Read corresponding LevelPageBitmap page
            let mut data = self.level_page_bitmap.read(data_info.data_id)?;
            data.truncate(data_info.data_len as usize);
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::utils::random_bytes32;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_kv_put_get_in_memory() {
        let dir = tempdir().unwrap();
        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        let key = random_bytes32().to_vec();
        let value = b"hello world".to_vec();

        kv.put(key.clone(), value.clone()).unwrap();

        // Directly read from in-memory buffer
        let result = kv.get(&key).unwrap();
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_kv_put_get_after_flush() {
        let dir = tempdir().unwrap();
        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        let key = random_bytes32().to_vec();
        let value = b"persisted data".to_vec();

        kv.put(key.clone(), value.clone()).unwrap();

        // Trigger flush
        // kv.trigger_async_flush();

        // Wait for flush thread to execute
        std::thread::sleep(Duration::from_secs(1));

        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        let result = kv.get(&key).unwrap();
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_kv_invalid_key_length() {
        let dir = tempdir().unwrap();
        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        let key = b"short".to_vec(); // Wrong length
        let value = b"oops".to_vec();

        let result = kv.put(key, value);
        assert!(matches!(result, Err(KVError::InvalidKeyLength)));
    }

    #[test]
    fn test_kv_large_key_count() {
        let dir = tempdir().unwrap();
        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        let mut keys = Vec::new();
        let value = vec![7u8; 64]; // Each small value of 64 bytes

        // Write 100_000 entries
        for i in 0..100_00 {
            let key = format!("key_{:012}", i).into_bytes();
            let mut fixed_key = key.clone();
            fixed_key.resize(32, 0);
            kv.put(fixed_key.clone(), value.clone()).unwrap();
            keys.push(fixed_key);
        }

        // Trigger flush
        //kv.trigger_async_flush();
        thread::sleep(Duration::from_secs(20));

        drop(kv);

        let kv = KV::new(dir.path(), KVOptions::default()).unwrap();

        // Random sample verification
        for i in (0..100_00).step_by(10_00) {
            let key = &keys[i];
            let result = kv.get(key).unwrap();
            assert!(result.is_some(), "key: {}", i);
            let data = result.unwrap();
            assert_eq!(data.len(), value.len());
            assert_eq!(&data[..16], &value[..16]); // Only compare prefix
        }
    }
}