use bitvec::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Mutex, RwLock};

#[derive(Debug)]
pub struct PageBitmap {
    meta_lock: Mutex<()>,
    levels: RwLock<Vec<BitVec<u8>>>, // levels[0] is the bottom, each bit represents a page
    page_size: u32,
    index_file: File,
    data_file: File,
}

impl PageBitmap {
    pub(crate) fn new(
        index_file_path: &Path,
        data_file_path: &Path,
        page_size: u32,
    ) -> std::io::Result<Self> {
        // Open or create index file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(index_file_path)?;
        // check file size
        if index_file_path.metadata()?.len() == 0 {
            let mut data_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(data_file_path)?;

            let data_size = 4096 * page_size;
            data_file.write_at(&[0], (data_size - 1) as u64)?;
            data_file.sync_all()?;

            let mut levels: Vec<BitVec<u8, Lsb0>> = Vec::new();
            levels.push(bitvec![u8, Lsb0; 0; 8 * 8 * 8 * 8]);
            levels.push(bitvec![u8, Lsb0; 0; 8 * 8 * 8]);
            levels.push(bitvec![u8, Lsb0; 0; 8 * 8]);
            levels.push(bitvec![u8, Lsb0; 0; 8]);

            // Initialize index file size = 4096 * page_size bits
            let total_size = 4096u64;
            file.write_at(&[0], total_size / 8 - 1)
                .expect("Failed to write zeros to initialize file");
            file.sync_all()?;

            Ok(Self {
                meta_lock: Mutex::new(()),
                levels: RwLock::new(levels),
                page_size,
                index_file: file,
                data_file,
            })
        } else {
            // Recover from existing files
            Self::recover_from_file(index_file_path, data_file_path, page_size)
        }
    }
    fn recover_from_file(
        index_file_path: &Path,
        data_file_path: &Path,
        page_size: u32,
    ) -> std::io::Result<Self> {
        let index_meta = std::fs::metadata(index_file_path)?;
        let data_meta = std::fs::metadata(data_file_path)?;
        if data_meta.len() % page_size as u64 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Index file size is not multiple of page_size",
            ));
        }
        if data_meta.len() != index_meta.len() * 8 * page_size as u64 {
            println!(
                "index_meta.len() * 8 * page_size = {}, data_meta.len() = {}",
                index_meta.len() * 8 * page_size as u64,
                data_meta.len()
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Data file size is not equal to index file size * 8 * page_size",
            ));
        }

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(index_file_path)?;
        let mut buffer = vec![0u8; index_meta.len() as usize];
        index_file.read_at(&mut buffer, 0)?;

        // Initialize bottom-level BitVec
        let mut levels = Vec::new();
        levels.push(BitVec::from_vec(buffer));

        // Build upper levels
        while levels.iter().last().unwrap().len() > 8 {
            let lower = levels.iter().last().unwrap();
            let mut upper = BitVec::new();
            // Every 8 bits in lower level corresponds to one bit in upper
            for chunk in lower.chunks(8) {
                upper.push(chunk.all()); // parent is 1 only if all children are 1
            }
            levels.push(upper);
        }

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_file_path)?;
        Ok(Self {
            meta_lock: Mutex::new(()),
            levels: RwLock::new(levels),
            page_size,
            index_file,
            data_file,
        })
    }

    // Allocate a new page
    fn allocate_page(&self) -> std::io::Result<u64> {
        let _guard = self.meta_lock.lock().unwrap();
        self.expand_if_need()?;
        let mut levels = self.levels.write().unwrap();
        drop(_guard);

        fn find_and_set(
            levels: &mut Vec<BitVec<u8>>,
            lvl: usize,
            start_index: usize,
        ) -> Option<usize> {
            let len = levels[lvl].len();
            for i in start_index..len {
                if lvl == 0 {
                    if !levels[lvl][i] {
                        levels[lvl].set(i, true);
                        return Some(i);
                    }
                } else {
                    if levels[lvl][i] {
                        continue;
                    }
                    let child_index = i * 8;
                    if let Some(found) = find_and_set(levels, lvl - 1, child_index) {
                        return Some(found);
                    }
                }
            }
            None
        }

        let lvl = levels.len() - 1;
        let allocated = find_and_set(&mut levels, lvl, 0)
            .map(|idx| idx as u64)
            .unwrap();

        // --- Update index file ---
        let byte_index = allocated / 8;
        let bit_index = allocated % 8;
        let mut buf = [0u8];
        self.index_file.read_at(&mut buf, byte_index)?;
        buf[0] |= 1 << bit_index;
        self.index_file.write_at(&buf, byte_index)?;

        // --- Update parent layers ---
        let mut idx = allocated as usize;
        for lvl in 0..levels.len() - 1 {
            let parent_idx = idx / 8;
            let child_range = parent_idx * 8..(parent_idx + 1) * 8;

            // If all 8 children are 1, mark parent as 1
            if levels[lvl][child_range.clone()].all() {
                levels[lvl + 1].set(parent_idx, true);
            } else {
                break;
            }
            idx = parent_idx;
        }
        Ok(allocated)
    }

    /// Expand PageBitmap if needed
    fn expand_if_need(&self) -> std::io::Result<()> {
        let mut levels_write = self.levels.write().unwrap();
        let mut top_level_idx = levels_write.len() - 1;
        let top_level = &levels_write[top_level_idx];

        // Check if expansion is needed
        let zero_count = top_level.iter().filter(|b| !**b).count();
        if zero_count > 1 {
            return Ok(()); // Expansion not needed
        }

        // 1️⃣ Expand files
        let mut increment = 1usize;
        for lvl in (0..=top_level_idx).rev() {
            let curr_level = &levels_write[lvl];
            let before_len = curr_level.len();
            increment *= 8;

            if lvl == 0 {
                let after_len = curr_level.len() + increment;
                expand_and_zero(
                    &self.index_file,
                    (before_len / 8) as u64,
                    (after_len / 8) as u64,
                )?;
                expand_and_zero(
                    &self.data_file,
                    before_len as u64 * self.page_size as u64,
                    (after_len as u64 * self.page_size as u64),
                )?;
            }
        }

        // 2️⃣ Expand memory levels
        let mut increment = 1usize;
        for lvl in (0..=top_level_idx).rev() {
            let curr_level = &mut levels_write[lvl];
            curr_level.extend(bitvec![0; increment]);
            increment *= 8;
        }

        // 3️⃣ Add new top level if current top reaches 64
        let top_level = &levels_write[levels_write.len() - 1];
        if top_level.len() == 64 {
            let mut new_top = bitvec![u8, Lsb0; 0; 8];
            let next_layer = &levels_write[levels_write.len() - 1];

            for i in 0..new_top.len() {
                let start = i * 8;
                let end = ((i + 1) * 8).min(next_layer.len());

                let all_ones = next_layer[start..end].iter().all(|b| *b);
                if all_ones {
                    new_top.set(i, true);
                }
            }

            levels_write.push(new_top);
        }
        Ok(())
    }

    /// Write data into a page
    pub fn write_page(&self, data: Vec<u8>) -> std::io::Result<u64> {
        let page_idx = self.allocate_page()? as usize;

        if data.len() > self.page_size as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Data length exceeds page_size",
            ));
        }

        let offset = (page_idx as u64) * self.page_size as u64;
        self.data_file.write_at(data.as_ref(), offset)?;

        Ok(page_idx as u64)
    }

    /// Read a page from file
    pub fn read_page(&self, page_idx: u64) -> std::io::Result<Vec<u8>> {
        let offset = page_idx * self.page_size as u64;
        let mut buffer = vec![0u8; self.page_size as usize];
        self.data_file.read_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    /// Free a page (mark as unused)
    pub fn free_page(&self, idx: u64) -> std::io::Result<()> {
        let idx_usize = idx as usize;

        // Clear bit in index file
        self.set_file_bit(idx_usize, false)?;

        let mut levels = self.levels.write().unwrap();
        levels[0].set(idx_usize, false);

        // Update parent levels
        let mut child_idx = idx_usize;
        for lvl in 0..levels.len() - 1 {
            let parent_idx = child_idx / 8;
            let child_range = parent_idx * 8..(parent_idx + 1) * 8;

            if levels[lvl][child_range.clone()].all() {
                levels[lvl + 1].set(parent_idx, true);
            } else {
                levels[lvl + 1].set(parent_idx, false);
            }

            child_idx = parent_idx;
        }

        Ok(())
    }

    /// Set or clear a bit in index file
    fn set_file_bit(&self, page_idx: usize, value: bool) -> std::io::Result<()> {
        let byte_index = page_idx / 8;
        let bit_index = page_idx % 8;

        let mut buf = [0u8];
        self.index_file.read_at(&mut buf, byte_index as u64)?;

        if value {
            buf[0] |= 1 << bit_index;
        } else {
            buf[0] &= !(1 << bit_index);
        }

        self.index_file.write_at(&buf, byte_index as u64)?;
        Ok(())
    }
}

/// Expand file from n1 to n2, ensuring new region is logically zeroed
pub fn expand_and_zero(file: &File, n1: u64, n2: u64) -> std::io::Result<()> {
    assert!(n2 >= n1, "n2 must be >= n1");

    if n2 == n1 {
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{FallocateFlags, fallocate};
        use std::os::unix::io::AsRawFd;

        let fd = file.as_raw_fd();
        fallocate(
            fd,
            FallocateFlags::FALLOC_FL_ZERO_RANGE,
            n1 as i64,
            (n2 - n1) as i64,
        )
            .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
        file.sync_all()?;
        return Ok(());
    }

    #[cfg(not(target_os = "linux"))]
    {
        let last_pos = n2 - 1;
        file.write_at(&[0], last_pos)?;
        file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_page_bitmap_basic_rw() {
        let dir = tempdir().unwrap();
        let index_file = dir.path().join("index.idx");
        let data_file = dir.path().join("data.dat");

        let page_size = 128u32;
        let bitmap = PageBitmap::new(&index_file, &data_file, page_size).unwrap();

        // Write one page
        let data = vec![1u8; page_size as usize];
        let page_idx = bitmap.write_page(data.clone()).unwrap();

        // Verify read
        let read_back = bitmap.read_page(page_idx).unwrap();
        assert_eq!(&read_back[..data.len()], &data[..]);

        // Free page
        bitmap.free_page(page_idx).unwrap();

        // After freeing, writing again should reuse the same page
        let page_idx2 = bitmap.write_page(data.clone()).unwrap();
        assert_eq!(page_idx, page_idx2);
    }

    #[test]
    fn test_write_page_exceed_size() {
        let dir = tempdir().unwrap();
        let index_file = dir.path().join("index.idx");
        let data_file = dir.path().join("data.dat");

        let page_size = 64u32;
        let bitmap = PageBitmap::new(&index_file, &data_file, page_size).unwrap();

        // Writing oversized page should fail
        let data = vec![1u8; (page_size + 1) as usize];
        let result = bitmap.write_page(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_recover_from_file() {
        let dir = tempdir().unwrap();
        let index_file = dir.path().join("index.idx");
        let data_file = dir.path().join("data.dat");

        let page_size = 128u32;

        {
            let bitmap = PageBitmap::new(&index_file, &data_file, page_size).unwrap();
            let data = vec![42u8; page_size as usize];
            let page_idx = bitmap.write_page(data.clone()).unwrap();

            let read_back = bitmap.read_page(page_idx).unwrap();
            assert_eq!(&read_back[..data.len()], &data[..]);
        }

        // Reload and ensure state can be recovered
        let recovered = PageBitmap::recover_from_file(&index_file, &data_file, page_size).unwrap();
        let data = vec![43u8; page_size as usize];
        let page_idx = recovered.write_page(data.clone()).unwrap();
        let read_back = recovered.read_page(page_idx).unwrap();
        assert_eq!(&read_back[..data.len()], &data[..]);
    }

    #[test]
    fn test_multiple_allocations_and_free() {
        let dir = tempdir().unwrap();
        let index_file = dir.path().join("index.idx");
        let data_file = dir.path().join("data.dat");

        let page_size = 64u32;
        let bitmap = PageBitmap::new(&index_file, &data_file, page_size).unwrap();

        // Allocate multiple pages continuously
        let mut pages = vec![];
        for i in 0..10 {
            let data = vec![i as u8; page_size as usize];
            let page_idx = bitmap.write_page(data.clone()).unwrap();
            pages.push((page_idx, data));
        }

        // Verify read correctness
        for (idx, data) in &pages {
            let read_back = bitmap.read_page(*idx).unwrap();
            assert_eq!(&read_back[..data.len()], &data[..]);
        }

        // Free first 5 pages
        for (idx, _) in pages.iter().take(5) {
            bitmap.free_page(*idx).unwrap();
        }

        // Write 5 more pages, should reuse freed ones
        for i in 0..5 {
            let data = vec![99u8; page_size as usize];
            let page_idx = bitmap.write_page(data.clone()).unwrap();
            let read_back = bitmap.read_page(page_idx).unwrap();
            assert_eq!(&read_back[..data.len()], &data[..]);
        }
    }

    #[test]
    fn test_multiple_allocations_and_free_with_expand_and_correctness() {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("index.idx");
        let data_path = dir.path().join("data.dat");

        // Page size = 16 bytes
        let bitmap = PageBitmap::new(&index_path, &data_path, 16).unwrap();

        let mut allocated = Vec::new();

        // Allocate 200,000 pages to ensure multiple expansions
        for i in 0..200_000 {
            let data = vec![(i % 256) as u8; 16]; // Different byte patterns
            let page_id = bitmap.write_page(data.clone()).unwrap();
            let read_back = bitmap.read_page(page_id).unwrap();
            assert_eq!(read_back, data, "data mismatch at page {}", i);

            // Only keep the first 1000 pages for free/reuse tests
            if i < 1000 {
                allocated.push((page_id, data));
            }

            // Optional: print progress
            if i % 20_000 == 0 {
                println!("Allocated {} pages", i);
            }
        }

        // Verify expansion has occurred
        {
            let levels = bitmap.levels.read().unwrap();
            assert!(
                levels[0].len() > 4096,
                "expand_if_need should have expanded the bottom level"
            );
        }

        // Free the first 500 pages
        for (id, _) in allocated.iter().take(500) {
            bitmap.free_page(*id).unwrap();
        }

        // Verify freed state is correct
        {
            let levels = bitmap.levels.read().unwrap();
            for (id, _) in allocated.iter().take(500) {
                assert!(!levels[0][*id as usize], "page {} should be freed", id);
            }
        }

        // Reallocate 500 pages to check reuse correctness
        for (i, (old_id, _)) in allocated.iter().take(500).enumerate() {
            let data = vec![0xAB; 16];
            let new_page_id = bitmap.write_page(data.clone()).unwrap();
            let read_back = bitmap.read_page(new_page_id).unwrap();
            assert_eq!(read_back, data, "re-allocation mismatch at {}", i);
        }
    }
}
