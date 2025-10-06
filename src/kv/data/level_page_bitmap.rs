use crate::kv::data::level_page_bitmap::page_bitmap::PageBitmap;
use serde::{Deserialize, Serialize};
use std::fs::{File, create_dir_all};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use moka::sync::Cache;

mod page_bitmap;

#[derive(Serialize, Deserialize, Default, Clone)]
struct FileMeta {
    page_size: u32,
    file_index: usize, // file index
}

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    // metadata of all files
    files: Vec<FileMeta>,
}

pub(crate) struct LevelPage {
    levels: Vec<PageBitmap>,
    levels_page_size: Vec<u32>,
    base_dir: PathBuf,
    meta: Meta,
}

#[derive(Clone)]
pub enum LevelsConfig {
    Pow2 {
        start_page_size: u32,
        level_count: u32,
    },
    Custom {
        level_page_sizes: Vec<u32>,
    },
}

#[derive(Clone)]
pub struct LevelPageOptions {
    pub levels_config: LevelsConfig,
    pub small_page_cache_size: u64,
}

const MIN_SMALL_PAGE_CACHE_SIZE: u64 = 64 * 1024 * 1024; //64MB

const SMALL_PAGE_SIZE_THRESHOLD: u64 = 2048;

impl Default for LevelPageOptions {
    fn default() -> Self {
        LevelPageOptions {
            levels_config: LevelsConfig::Pow2 {
                start_page_size: 32,
                level_count: 8,
            },
            small_page_cache_size: MIN_SMALL_PAGE_CACHE_SIZE, // 64MB
        }
    }
}

impl LevelPage {
    pub fn new(base_dir: impl Into<PathBuf>, opts: LevelPageOptions) -> std::io::Result<Self> {
        let base_dir = base_dir.into();
        create_dir_all(&base_dir)?;

        let meta_path = base_dir.join("meta.json");

        let meta: Meta = if meta_path.exists() {
            let file = File::open(&meta_path)?;
            serde_json::from_reader(BufReader::new(file))?
        } else {
            let level_page_sizes = match opts.levels_config {
                LevelsConfig::Pow2 {
                    start_page_size,
                    level_count,
                } => {
                    let mut level_page_sizes = Vec::with_capacity(level_count as usize);
                    level_page_sizes.push(start_page_size as u32);
                    for i in 1..=level_count-1 {
                        level_page_sizes.push((level_page_sizes[i as usize - 1] * 2));
                    }
                    level_page_sizes
                }
                LevelsConfig::Custom { level_page_sizes } => level_page_sizes,
            };

            let mut files = Vec::new();

            for (i, level_page_size) in level_page_sizes.iter().enumerate() {
                files.push(FileMeta {
                    page_size: *level_page_size,
                    file_index: i,
                })
            }
            let meta = Meta { files };
            let file = File::create(&meta_path)?;
            serde_json::to_writer_pretty(BufWriter::new(file), &meta)?;
            meta
        };

        // recover PageBitmap
        let mut levels = Vec::new();
        let mut levels_page_size = Vec::new();
        
        let mut cache_size = opts.small_page_cache_size;
        if cache_size < MIN_SMALL_PAGE_CACHE_SIZE {
            cache_size = MIN_SMALL_PAGE_CACHE_SIZE;
        }
        let shared_cache = Arc::new(
            Cache::builder()
                .max_capacity(cache_size)
                .weigher(|_k: &u64, v: &Vec<u8>| v.len() as u32)
                .build(),
        );
        
        for file_meta in &meta.files {
            let index_path = base_dir.join(format!(
                "index_{}b_{}.idx",
                file_meta.page_size, file_meta.file_index
            ));
            let data_path = base_dir.join(format!(
                "data_{}b_{}.dat",
                file_meta.page_size, file_meta.file_index
            ));

            if file_meta.page_size <= SMALL_PAGE_SIZE_THRESHOLD as u32 {
                let page_bitmap = PageBitmap::new(&index_path, &data_path, file_meta.page_size, Some(shared_cache.clone()))?;
                levels.push(page_bitmap);
            } else { 
                let page_bitmap = PageBitmap::new(&index_path, &data_path, file_meta.page_size, None)?;
                levels.push(page_bitmap);
            }
            
            

            if !levels_page_size.contains(&file_meta.page_size) {
                levels_page_size.push(file_meta.page_size);
            }
        }

        Ok(Self {
            levels,
            levels_page_size,
            base_dir,
            meta,
        })
    }

    /// Write data into the most suitable PageBitmap
    pub fn write(&self, value: Vec<u8>) -> std::io::Result<u64> {
        let size = value.len() as u32;
        assert!(size <= *self.levels_page_size.last().unwrap());

        // find corresponding level
        let mut target_level_idx = None;
        for (i, &ps) in self.levels_page_size.iter().enumerate() {
            if ps >= size {
                target_level_idx = Some(i);
                break;
            }
        }
        let level_idx = target_level_idx.unwrap();

        let page_idx = self.levels[level_idx].write_page(value)?;

        // encode data_id: high 8 bits store level index
        let encoded = ((level_idx as u64) << 56) | (page_idx & 0x00FFFFFFFFFFFFFF);
        Ok(encoded)
    }

    pub fn free(&self, data_id: u64) -> std::io::Result<()> {
        let level_idx = (data_id >> 56) as usize;
        let page_idx = data_id & 0x00FFFFFFFFFFFFFF;
        self.levels[level_idx].free_page(page_idx)?;
        Ok(())
    }

    /// Read data
    pub fn read(&self, data_id: u64) -> std::io::Result<Vec<u8>> {
        let level = (data_id >> 56) as usize;
        let page_idx = data_id & 0x00FFFFFFFFFFFFFF;

        if level >= self.levels.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid level index: {}", level),
            ));
        }

        self.levels[level].read_page(page_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_level_page_bitmap_basic() {
        use crate::kv::data::level_page_bitmap::LevelPage;

        let dir = TempDir::new().unwrap();
        let base_dir = dir.path();

        let lpb = LevelPage::new(base_dir, LevelPageOptions::default()).unwrap();

        let data_small = vec![0xAA; 16]; // fits in small page_size
        let data_medium = vec![0xBB; 64]; // fits in medium page_size
        let data_large = vec![0xCC; 4096]; // fits in maximum page_size

        let id_small = lpb.write(data_small.clone()).unwrap();
        let id_medium = lpb.write(data_medium.clone()).unwrap();
        let id_large = lpb.write(data_large.clone()).unwrap();

        let read_small = lpb.read(id_small).unwrap();
        let read_medium = lpb.read(id_medium).unwrap();
        let read_large = lpb.read(id_large).unwrap();

        assert_eq!(
            &read_small[..data_small.len()],
            &data_small[..],
            "small data content mismatch"
        );
        assert_eq!(
            &read_medium[..data_medium.len()],
            &data_medium[..],
            "medium data content mismatch"
        );
        assert_eq!(
            &read_large[..data_large.len()],
            &data_large[..],
            "large data content mismatch"
        );

        // reopen LevelPageBitmap, verify recovery
        let lpb_recovered = LevelPage::new(base_dir, LevelPageOptions::default()).unwrap();

        let read_small_re = lpb_recovered.read(id_small).unwrap();
        let read_medium_re = lpb_recovered.read(id_medium).unwrap();
        let read_large_re = lpb_recovered.read(id_large).unwrap();

        assert_eq!(
            &read_small[..data_small.len()],
            &data_small[..],
            "small data content mismatch"
        );
        assert_eq!(
            &read_medium[..data_medium.len()],
            &data_medium[..],
            "medium data content mismatch"
        );
        assert_eq!(
            &read_large[..data_large.len()],
            &data_large[..],
            "large data content mismatch"
        );
    }

    #[test]
    fn test_level_page_bitmap_large_volume() {
        use super::*;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let base_dir = dir.path();

        let lpb = LevelPage::new(base_dir, LevelPageOptions::default()).unwrap();

        let num_entries = 10_0000;
        let mut data_ids = Vec::with_capacity(num_entries);

        use rand::Rng;
        let mut rng = rand::thread_rng();

        for _ in 0..num_entries {
            let len = rng.gen_range(32..=4096);
            let data = vec![0; len];
            let id = lpb.write(data.clone()).unwrap();
            data_ids.push((id, data));
        }

        for (id, original) in &data_ids {
            let read_back = lpb.read(*id).unwrap();
            assert_eq!(
                &read_back[..original.len()],
                &original[..],
                "data mismatch for id {}",
                id
            );
        }

        // reopen and verify recovery
        let lpb_recovered = LevelPage::new(base_dir, LevelPageOptions::default()).unwrap();

        for (id, original) in &data_ids {
            let read_back = lpb_recovered.read(*id).unwrap();
            assert_eq!(
                &read_back[..original.len()],
                &original[..],
                "recovered data mismatch for id {}",
                id
            );
        }
    }
}
