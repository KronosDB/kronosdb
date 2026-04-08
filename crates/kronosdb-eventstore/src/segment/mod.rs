pub mod format;
pub mod writer;
pub mod reader;
pub mod segment_index;

use std::path::{Path, PathBuf};

/// Magic bytes at the start of every segment file: "KRON"
pub const SEGMENT_MAGIC: [u8; 4] = *b"KRON";

/// Current segment format version.
pub const SEGMENT_VERSION: u8 = 1;

/// Segment file header size in bytes.
/// Layout: [4 magic] [1 version] [8 base_position] = 13 bytes
pub const SEGMENT_HEADER_SIZE: usize = 13;

/// Record header size in bytes.
/// Layout: [4 CRC32C] [4 record_len] [1 flags] = 9 bytes
/// record_len covers: flags (1 byte) + payload (N bytes)
pub const RECORD_HEADER_SIZE: usize = 9;

/// Default segment size: 256 MB (matching Axon Server).
pub const DEFAULT_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;

/// Record flags.
pub mod flags {
    /// No special flags.
    pub const NONE: u8 = 0x00;
    /// This record is part of a transaction batch (reserved for future use).
    pub const BATCH: u8 = 0x01;
}

/// Generates the segment file path for a given base position.
/// Format: {base_position:020}.seg
pub fn segment_path(dir: &Path, base_position: u64) -> PathBuf {
    dir.join(format!("{:020}.seg", base_position))
}

/// Extracts the base position from a segment file name.
/// Returns None if the file name doesn't match the expected format.
pub fn base_position_from_path(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    if path.extension()?.to_str()? != "seg" {
        return None;
    }
    stem.parse::<u64>().ok()
}

/// Lists all segment base positions in a directory, sorted ascending.
pub fn list_segment_files(dir: &Path) -> Result<Vec<u64>, std::io::Error> {
    let mut positions = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(base) = base_position_from_path(&entry.path()) {
            positions.push(base);
        }
    }
    positions.sort();
    Ok(positions)
}
