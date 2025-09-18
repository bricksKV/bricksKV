use crate::kv::utils::{create_file_with_len, remove_file_if_exists};
use dashmap::DashMap;
use std::fs::{File, OpenOptions, rename};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

pub trait BucketValue: Sized {
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: &[u8]) -> Option<Self>;
}

/// Error type
#[derive(Debug)]
pub enum BucketError {
    Io(io::Error),
    MaxSearchReached,
    InvalidKeyLength,
    Other(String),
}

impl std::fmt::Display for BucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BucketError::Io(e) => write!(f, "IO error: {}", e),
            BucketError::MaxSearchReached => write!(f, "Maximum search limit reached"),
            BucketError::InvalidKeyLength => write!(f, "Key length does not match"),
            BucketError::Other(s) => write!(f, "Other error: {}", s),
        }
    }
}

impl std::error::Error for BucketError {}

impl From<io::Error> for BucketError {
    fn from(e: io::Error) -> Self {
        BucketError::Io(e)
    }
}

/// Entry state, represented by a single u8
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum EntryMeta {
    Free = 0,
    Occupied = 1,
}

impl EntryMeta {
    pub fn new_free() -> Self {
        EntryMeta::Free
    }
    pub fn new_occupied() -> Self {
        EntryMeta::Occupied
    }
}

/// A single entry
pub struct Entry<T: BucketValue> {
    meta: EntryMeta,
    key: Vec<u8>,
    value: T,
}

impl<T: BucketValue> Entry<T> {
    pub fn encode(&self, key_size: usize) -> Vec<u8> {
        assert_eq!(self.key.len(), key_size, "Key length must be fixed");
        let value_bytes = self.value.encode();
        let mut buf = Vec::with_capacity(1 + key_size + value_bytes.len());
        buf.push(self.meta as u8);
        buf.extend(&self.key);
        buf.extend(value_bytes);
        buf
    }

    pub fn decode(bytes: &[u8], key_size: usize) -> Option<Self> {
        if bytes.len() <= 1 + key_size {
            return None;
        }
        let meta = match bytes[0] {
            0 => EntryMeta::Free,
            1 => EntryMeta::Occupied,
            _ => return None,
        };
        let key = bytes[1..1 + key_size].to_vec();
        let value = T::decode(&bytes[1 + key_size..])?;
        Some(Self { meta, key, value })
    }

    pub fn entry_size(key_size: u32, value_size: usize) -> u32 {
        1 + key_size + value_size as u32
    }

    pub fn is_free(&self) -> bool {
        matches!(self.meta, EntryMeta::Free)
    }

    pub fn is_occupied(&self) -> bool {
        matches!(self.meta, EntryMeta::Occupied)
    }

    pub fn set_free(&mut self) {
        self.meta = EntryMeta::Free;
    }

    pub fn set_occupied(&mut self) {
        self.meta = EntryMeta::Occupied;
    }
}

/// Data info
#[derive(Clone, Debug)]
pub struct DataInfo {
    pub data_id: u64,
    pub data_len: u32,
}

struct InnerData {
    file: File,
    entry_num: u64,
}

/// Hash bucket
pub struct Bucket<T: BucketValue> {
    inner_data: RwLock<InnerData>,
    dir: PathBuf,
    key_size: u32,
    entry_size: u32,
    _marker: std::marker::PhantomData<T>,
}

const MAX_SEARCH_DEFAULT: usize = 32;

#[derive(Debug)]
pub enum RehashError {
    Io(io::Error),
    MaxSearchExceeded,
}

impl From<io::Error> for RehashError {
    fn from(e: io::Error) -> Self {
        RehashError::Io(e)
    }
}
const DEFAULT_FILE_NAME: &str = "bucket.dat";
impl<T: BucketValue> Bucket<T> {
    pub fn new<P: AsRef<Path>>(
        dir: P,
        key_size: u32,
        value_size: u32,
        init_entry_num: u32,
    ) -> Result<Self, BucketError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?; // Ensure directory exists

        let path = dir.join(DEFAULT_FILE_NAME);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let entry_size = Entry::<T>::entry_size(key_size, value_size as usize);
        let mut file_len = file.metadata()?.len();

        if file_len == 0 {
            let total_size = entry_size as u64 * init_entry_num as u64;
            file.seek(SeekFrom::Start(total_size - 1))?;
            file.write_all(&[0])?;
            file.rewind()?;
            file_len = file.metadata()?.len();
        }

        let inner_data = RwLock::new(InnerData {
            file,
            entry_num: file_len / entry_size as u64,
        });

        Ok(Self {
            inner_data,
            dir: dir.to_path_buf(),
            key_size,
            entry_size,
            _marker: std::marker::PhantomData,
        })
    }

    fn get_max_search(&self) -> usize {
        MAX_SEARCH_DEFAULT
    }

    /// Read multiple entries at once
    fn read_entries(&self, start_index: u64, count: usize) -> Result<Vec<Entry<T>>, BucketError> {
        let mut entries = Vec::with_capacity(count);
        let mut buf = vec![0u8; self.entry_size as usize * count];

        for i in 0..count {
            let index = (start_index + i as u64) % self.inner_data.read().unwrap().entry_num;
            let offset = index as u64 * self.entry_size as u64;
            self.inner_data.read().unwrap().file.read_at(
                &mut buf[i * self.entry_size as usize..(i + 1) * self.entry_size as usize],
                offset,
            )?;
        }

        for i in 0..count {
            let entry_bytes =
                &buf[i * self.entry_size as usize..(i + 1) * self.entry_size as usize];
            let entry = Entry::<T>::decode(entry_bytes, self.key_size as usize).unwrap();
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn put(&self, key: Vec<u8>, value: T) -> Result<(), BucketError> {
        if key.len() != self.key_size as usize {
            return Err(BucketError::InvalidKeyLength);
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let inner_data_with_read_lock = self.inner_data.read().unwrap();

        let start_index = hash % inner_data_with_read_lock.entry_num;
        let max_search = self.get_max_search();

        for i in 0..max_search {
            let index = (start_index + i as u64) % inner_data_with_read_lock.entry_num;
            let offset = index * self.entry_size as u64;

            let mut buf = vec![0u8; self.entry_size as usize];
            inner_data_with_read_lock
                .file
                .read_at(&mut buf, offset)?;
            let entry = Entry::<T>::decode(&buf, self.key_size as usize).unwrap();

            if entry.is_free() || entry.key == key {
                let new_entry = Entry {
                    meta: EntryMeta::Occupied,
                    key: key.clone(),
                    value,
                };
                let encoded = new_entry.encode(self.key_size as usize);
                inner_data_with_read_lock
                    .file
                    .write_all_at(&encoded, offset)?;
                return Ok(());
            }
        }

        Err(BucketError::MaxSearchReached)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<T>, BucketError> {
        if key.len() != self.key_size as usize {
            return Err(BucketError::InvalidKeyLength);
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let inner_data_with_read_lock = self.inner_data.read().unwrap();
        let start_index = hash % inner_data_with_read_lock.entry_num;
        let max_search = self.get_max_search();

        for i in 0..max_search {
            let index = (start_index + i as u64) % inner_data_with_read_lock.entry_num;
            let offset = index * self.entry_size as u64;

            let mut buf = vec![0u8; self.entry_size as usize];
            inner_data_with_read_lock
                .file
                .read_at(&mut buf, offset)?;
            let entry = Entry::<T>::decode(&buf, self.key_size as usize).unwrap();

            if entry.is_occupied() && entry.key == key {
                return Ok(Some(entry.value));
            }
        }
        Ok(None)
    }

    pub fn del(&self, key: &[u8]) -> Result<Option<T>, BucketError> {
        if key.len() != self.key_size as usize {
            return Err(BucketError::InvalidKeyLength);
        }
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let inner_data_with_read_lock = self.inner_data.read().unwrap();
        let start_index = hash % inner_data_with_read_lock.entry_num as u64;
        let max_search = self.get_max_search();

        for i in 0..max_search {
            let index = (start_index + i as u64) % inner_data_with_read_lock.entry_num;
            let offset = index * self.entry_size as u64;

            let mut buf = vec![0u8; self.entry_size as usize];
            inner_data_with_read_lock.file.read_at(&mut buf, offset)?;
            let mut entry = Entry::<T>::decode(&buf, self.key_size as usize).unwrap();

            if entry.is_occupied() && entry.key == key {
                entry.set_free();
                let encoded = entry.encode(self.key_size as usize);
                inner_data_with_read_lock.file.write_at(&encoded, offset)?;
                return Ok(Some(entry.value));
            }
        }

        Ok(None)
    }

    pub fn expand(&self) -> Result<(), BucketError> {
        let mut new_entry_num = self.inner_data.read().unwrap().entry_num;
        loop {
            new_entry_num *= 2;
            match self.do_expand(new_entry_num) {
                Ok(_) => {
                    return Ok(())
                }
                Err(err) => {
                    match err {
                        BucketError::MaxSearchReached => {
                            continue;
                        }
                        _ => return Err(err),
                    }
                }
            }
        }
    }

    pub fn do_expand(&self, new_entry_num: u64) -> Result<(), BucketError> {
        let tmp_path = self.dir.join(DEFAULT_FILE_NAME.to_owned() + ".tmp");

        {
            let inner_data_with_read_lock = self.inner_data.read().unwrap();

            let entry_size = self.entry_size as usize;
            let key_size = self.key_size as usize;
            remove_file_if_exists(&tmp_path)?;

            // Initialize new file
            let new_file_len = new_entry_num * self.entry_size as u64;
            let new_file = create_file_with_len(&tmp_path, new_file_len)?;

            // Migrate occupied entries
            for i in 0..inner_data_with_read_lock.entry_num {
                let offset = i * self.entry_size as u64;
                let mut buf = vec![0u8; entry_size];
                inner_data_with_read_lock
                    .file
                    .read_at(&mut buf, offset)?;
                let entry = Entry::<T>::decode(&buf, key_size).unwrap();

                if entry.is_occupied() {
                    let mut hasher = DefaultHasher::new();
                    entry.key.hash(&mut hasher);
                    let hash = hasher.finish();
                    let mut new_index = (hash % new_entry_num) as usize;

                    let mut searched = 0;
                    while searched < MAX_SEARCH_DEFAULT {
                        let new_offset = new_index as u64 * self.entry_size as u64;
                        let mut new_buf = vec![0u8; entry_size];
                        new_file.read_at(&mut new_buf, new_offset)?;
                        let new_entry = Entry::<T>::decode(&new_buf, key_size).unwrap();
                        if new_entry.is_free() {
                            let encoded = entry.encode(key_size);
                            new_file.write_all_at(&encoded, new_offset)?;
                            break;
                        }
                        new_index = (new_index + 1) % new_entry_num as usize;
                        searched += 1;
                    }

                    if searched >= MAX_SEARCH_DEFAULT {
                        return Err(BucketError::MaxSearchReached);
                    }
                }
            }
        }
        rename(&tmp_path, &self.dir.join(DEFAULT_FILE_NAME.to_owned()))?;

        let path = self.dir.join(DEFAULT_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        let mut inner_data_with_write_lock = self.inner_data.write().unwrap();
        inner_data_with_write_lock.file = file;
        inner_data_with_write_lock.entry_num = new_entry_num;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[derive(Clone, Debug, PartialEq)]
    struct TestValue {
        a: u64,
        b: u32,
    }

    impl BucketValue for TestValue {
        fn encode(&self) -> Vec<u8> {
            let mut buf = Vec::with_capacity(12);
            buf.extend(&self.a.to_le_bytes());
            buf.extend(&self.b.to_le_bytes());
            buf
        }

        fn decode(bytes: &[u8]) -> Option<Self> {
            if bytes.len() < 12 {
                return None;
            }
            let a = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
            let b = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
            Some(TestValue { a, b })
        }
    }

    #[test]
    fn test_bucket_put_get_del() -> Result<(), BucketError> {
        let dir = tempdir().unwrap();
        println!("dir: {}", dir.path().display());
        let key_size = 8u32;
        let value_size = 12; // TestValue takes 12 bytes
        let init_entry_num = 16;

        let bucket = Bucket::<TestValue>::new(dir.path(), key_size, value_size, init_entry_num)?;

        // put
        let key = b"key00001".to_vec();
        let value = TestValue { a: 123, b: 456 };
        bucket.put(key.clone(), value.clone())?;

        // get
        let result = bucket.get(&key)?.unwrap();
        assert_eq!(result, value);

        // del
        let deleted = bucket.del(&key)?;
        assert!(deleted.is_some());

        // get after del
        let result = bucket.get(&key)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_bucket_expand_fixed_key_size() -> Result<(), BucketError> {
        let dir = tempdir().unwrap();
        let key_size = 8u32; // key fixed to 8 bytes
        let value_size = 12;
        let init_entry_num = 4; // Small capacity to trigger expand

        let bucket = Bucket::<TestValue>::new(dir.path(), key_size, value_size, init_entry_num)?;

        // Insert 4 values
        for i in 0..4 {
            let key_str = format!("{:0>8}", i); // Generate 8-byte key like "00000000"
            let key = key_str.as_bytes().to_vec();
            let value = TestValue {
                a: i as u64,
                b: (i * 10) as u32,
            };
            bucket.put(key, value)?;
        }

        // Expand
        bucket.expand()?;

        // Insert new value after expansion
        let key = b"key00001".to_vec(); // 8 bytes
        let value = TestValue { a: 999, b: 9999 };
        bucket.put(key.clone(), value.clone())?;

        let result = bucket.get(&key)?.unwrap();
        assert_eq!(result, value);

        // Verify previously inserted values are still retrievable
        for i in 0..4 {
            let key_str = format!("{:0>8}", i);
            let key = key_str.as_bytes().to_vec();
            let expected = TestValue { a: i as u64, b: (i * 10) as u32 };
            let got = bucket.get(&key)?.unwrap();
            assert_eq!(got, expected);
        }

        Ok(())
    }
}