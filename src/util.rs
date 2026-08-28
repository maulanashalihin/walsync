use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::SystemTime;

/// WAL header size in bytes
pub const WAL_HEADER_SIZE: u64 = 32;
/// WAL frame header size in bytes
pub const WAL_FRAME_HEADER_SIZE: u64 = 24;

/// Split comma-separated string into trimmed non-empty parts
pub fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// Get file size, returns 0 if file doesn't exist
pub fn file_size(path: &str) -> i64 {
    fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0)
}

/// Get file modification time as SystemTime
pub fn file_mod_time(path: &str) -> SystemTime {
    fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

/// Read WAL salt values (bytes 16-23) from WAL header.
/// Returns (0, 0) if WAL file doesn't exist or is too small.
pub fn wal_salt(wal_path: &str) -> (u32, u32) {
    let mut f = match fs::File::open(wal_path) {
        Ok(f) => f,
        Err(_) => return (0, 0),
    };
    let mut header = [0u8; 32];
    if f.read_exact(&mut header).is_err() {
        return (0, 0);
    }
    let salt1 = u32::from_be_bytes(header[16..20].try_into().unwrap());
    let salt2 = u32::from_be_bytes(header[20..24].try_into().unwrap());
    (salt1, salt2)
}

/// Check if address has a port (ends with :<digits>)
pub fn has_port(addr: &str) -> bool {
    if let Some(colon) = addr.rfind(':') {
        let after = &addr[colon + 1..];
        !after.is_empty() && after.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Ensure address has a port, default to 9090
pub fn ensure_port(addr: &str) -> String {
    if has_port(addr) {
        addr.to_string()
    } else {
        format!("{}:9090", addr)
    }
}

/// Read a range of bytes from a file starting at offset
pub fn read_file_range(path: &str, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
    let mut f = fs::File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    let n = f.read(&mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

/// Write data to file at offset (create if needed)
pub fn write_file_at(path: &str, offset: i64, data: &[u8]) -> std::io::Result<()> {
    let mut f = if offset == 0 {
        fs::File::create(path)?
    } else {
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)?
    };
    if offset > 0 {
        f.seek(SeekFrom::Start(offset as u64))?;
    }
    f.write_all(data)?;
    Ok(())
}

/// Atomically replace DB file: write to .tmp, then rename
pub fn atomic_replace_db(db_path: &str, data: &[u8]) -> std::io::Result<()> {
    let tmp_path = format!("{}.tmp", db_path);
    fs::write(&tmp_path, data)?;
    // Remove stale WAL and SHM
    let _ = fs::remove_file(format!("{}-wal", db_path));
    let _ = fs::remove_file(format!("{}-shm", db_path));
    fs::rename(&tmp_path, db_path)?;
    Ok(())
}
