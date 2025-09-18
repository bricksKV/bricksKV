use crate::kv::index::bucket::{Bucket, BucketError, BucketValue};
use crate::kv::utils::create_dir_if_not_exists;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{File, OpenOptions, create_dir_all};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::{fmt, io};

const DEFAULT_BUCKET_COUNT: u32 = 32;

#[derive(Serialize, Deserialize)]
struct BucketMeta {
    path: String,
    exists: bool,
}

#[derive(Serialize, Deserialize)]
struct BucketsMeta {
    bucket_count: u32,
    key_size: u32,
}

pub struct Buckets<T: BucketValue> {
    buckets: boxcar::Vec<RwLock<Bucket<T>>>,
    key_size: u32,
    bucket_count: u32,
    base_dir: PathBuf,
}

#[derive(Debug)]
pub enum BucketsError {
    Io(io::Error),
    Other(String),
    InvalidKeyLength,
    MaxSearchReached,
}

impl From<BucketError> for BucketsError {
    fn from(err: BucketError) -> Self {
        match err {
            BucketError::Io(e) => BucketsError::Io(e),
            BucketError::MaxSearchReached => BucketsError::MaxSearchReached,
            BucketError::InvalidKeyLength => BucketsError::InvalidKeyLength,
            BucketError::Other(s) => BucketsError::Other(s),
        }
    }
}

impl fmt::Display for BucketsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketsError::Io(e) => write!(f, "IO error: {}", e),
            BucketsError::Other(s) => write!(f, "Unknown error: {}", s),
            BucketsError::InvalidKeyLength => write!(f, "Key size does not match"),
            BucketsError::MaxSearchReached => write!(f, "Max search limit reached"),
        }
    }
}

impl Error for BucketsError {}

impl From<io::Error> for BucketsError {
    fn from(e: io::Error) -> Self {
        BucketsError::Io(e)
    }
}

impl From<serde_json::Error> for BucketsError {
    fn from(e: serde_json::Error) -> Self {
        BucketsError::Other(format!("Serde error: {}", e))
    }
}

pub struct BucketsOptions {
    pub key_size: u32,
    pub bucket_count: u32,
    pub init_entry_num_for_each_bucket: u32,
}

impl Default for BucketsOptions {
    fn default() -> Self {
        BucketsOptions {
            key_size: 32,
            bucket_count: DEFAULT_BUCKET_COUNT,
            init_entry_num_for_each_bucket: 1024,
        }
    }
}

impl<T: BucketValue + Clone> Buckets<T> {
    pub fn new<P: AsRef<Path>>(base_dir: P, opts: BucketsOptions) -> Result<Self, BucketsError> {
        create_dir_all(&base_dir)?;
        let base_dir = base_dir.as_ref().to_path_buf();
        let meta_path = base_dir.join("meta.json");

        let mut buckets = boxcar::Vec::with_capacity(opts.bucket_count as usize);
        let mut meta: BucketsMeta = if meta_path.exists() {
            let file = File::open(&meta_path)?;
            serde_json::from_reader(file)?
        } else {
            // Metadata does not exist, create a new one
            // Write metadata file to ensure recovery on restart
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&meta_path)?;
            let meta = BucketsMeta {
                bucket_count: opts.bucket_count,
                key_size: opts.key_size,
            };
            serde_json::to_writer_pretty(file, &meta)?;
            meta
        };

        for i in 0..meta.bucket_count {
            let path = base_dir.join(format!("bucket_{:05}.data", i));
            create_dir_if_not_exists(path.clone())?;
            // If file already exists, restore
            let bucket = Bucket::new(
                &path,
                opts.key_size,
                size_of::<T>() as u32,
                opts.init_entry_num_for_each_bucket,
            )?;
            buckets.push(RwLock::new(bucket));
        }

        Ok(Self {
            buckets,
            key_size: opts.key_size,
            bucket_count: opts.bucket_count,
            base_dir,
        })
    }

    fn hash_key(&self, key: &[u8]) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count as usize
    }

    pub fn put(&self, key: Vec<u8>, value: T) -> Result<(), BucketsError> {
        loop {
            let idx = self.hash_key(&key);
            let mut bucket = self.buckets[idx].write().unwrap();
            match bucket.put(key.clone(), value.clone()) {
                Ok(_) => return Ok(()),
                Err(err) => {
                    match err {
                        BucketError::MaxSearchReached => {
                            // If bucket is full, trigger expansion
                            bucket.expand()?;
                            continue;
                        }
                        _ => return Err(err.into()),
                    }
                }
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<T>, BucketsError> {
        let idx = self.hash_key(key);
        let bucket = self.buckets[idx].read().unwrap();
        Ok(bucket.get(key)?)
    }

    pub fn del(&self, key: &Vec<u8>) -> Result<Option<T>, BucketsError> {
        let idx = self.hash_key(key);
        let mut bucket = self.buckets[idx].write().unwrap();
        Ok(bucket.del(key)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    fn test_buckets_put_get_del() -> Result<(), BucketsError> {
        let dir = tempdir().unwrap();
        let buckets = Buckets::<TestValue>::new(dir.path(), BucketsOptions::default())?;

        // 生成一个 32 字节的 key
        let key = format!("{:0>32}", "key00001").as_bytes().to_vec();
        assert_eq!(key.len(), 32);

        let value = TestValue { a: 123, b: 456 };

        // put
        buckets.put(key.clone(), value.clone())?;

        // get
        let result = buckets.get(&key)?.unwrap();
        assert_eq!(result, value);

        // del
        buckets.del(&key)?;

        // get after del
        let result = buckets.get(&key)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_buckets_large_data() -> Result<(), BucketsError> {
        let dir = tempdir().unwrap();
        let buckets = Buckets::<TestValue>::new(dir.path(), BucketsOptions::default())?;

        let total = 500000;
        let mut keys = Vec::new();

        // Insert large amount of data
        for i in 0..total {
            let key_str = format!("{:0>32}", i); // Fixed-length key
            let key = key_str.as_bytes().to_vec();
            let value = TestValue {
                a: i as u64,
                b: (i * 10) as u32,
            };
            buckets.put(key.clone(), value.clone())?;
            keys.push((key, value));
        }

        // Randomly sample for validation
        for (i, (key, value)) in keys.iter().step_by(777).enumerate() {
            let got = buckets.get(key)?.unwrap();
            assert_eq!(got, *value, "mismatch at sample {}", i);
        }

        // Delete a subset
        for (i, (key, _)) in keys.iter().enumerate().take(1000) {
            buckets.del(key)?;
            let result = buckets.get(key)?;
            assert!(result.is_none(), "key {:?} should be deleted", key);
        }

        Ok(())
    }
}