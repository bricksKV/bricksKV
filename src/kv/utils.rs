use std::{fs, io};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use rand::Rng;

/// Create a directory if it does not exist
pub fn create_dir_if_not_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::create_dir(&path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()), // Ignore error if already exists
        Err(e) => Err(e), // Return other errors
    }
}

/// Remove a file if it exists
pub fn remove_file_if_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()), // Ignore if file does not exist
        Err(e) => Err(e), // Return other errors
    }
}

/// Create a new file with a fixed length
pub fn create_file_with_len<P: AsRef<Path>>(path: P, size: u64) -> io::Result<std::fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)       // Allow writing
        .create_new(true)  // Must be a new file, otherwise error
        .open(path)?;

    // Set the file length
    file.set_len(size)?;

    // Ensure the last byte is written as 0 (avoid sparse file issue)
    let mut file = file;
    if size > 0 {
        file.seek(SeekFrom::Start(size - 1))?;
        file.write_all(&[0])?;
    }
    Ok(file)
}

/// Check if a path exists
pub fn path_exist(path: &Path) -> io::Result<bool> {
    path.try_exists()
}

/// Generate 32 random bytes
pub fn random_bytes32() -> [u8; 32] {
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    bytes
}