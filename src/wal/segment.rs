use apollo_telemetry_protocol::{invalid, local::private_file};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};
pub const HEADER: u64 = 32;
#[derive(Clone, Copy)]
pub struct Segment {
    pub id: u64,
    pub size: u64,
    pub durable: u64,
}
pub fn path(root: &Path, id: u64) -> PathBuf {
    root.join(format!("segment-{id:020}.wal"))
}
pub fn create(root: &Path, id: u64) -> io::Result<(File, Segment)> {
    let target = path(root, id);
    if target.exists() {
        return Err(invalid());
    }
    let temporary = root.join("segment-new");
    let file = private_file(&temporary)?;
    file.set_len(0)?;
    let mut header = [0; 32];
    header[..8].copy_from_slice(b"ALWAL01\0");
    header[8..10].copy_from_slice(&1u16.to_le_bytes());
    header[16..24].copy_from_slice(&id.to_le_bytes());
    let crc = crc32fast::hash(&header[..28]);
    header[28..].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&header, 0)?;
    file.sync_data()?;
    fs::rename(&temporary, &target)?;
    File::open(root)?.sync_all()?;
    Ok((
        file,
        Segment {
            id,
            size: HEADER,
            durable: HEADER,
        },
    ))
}
pub fn inspect(file: &File) -> io::Result<Segment> {
    let size = file.metadata()?.len();
    if size < HEADER {
        return Err(invalid());
    }
    let mut header = [0; 32];
    file.read_exact_at(&mut header, 0)?;
    let mut id = [0; 8];
    id.copy_from_slice(&header[16..24]);
    let id = u64::from_le_bytes(id);
    let mut crc = [0; 4];
    crc.copy_from_slice(&header[28..]);
    if header[..8] != *b"ALWAL01\0"
        || header[8..10] != 1u16.to_le_bytes()
        || header[10..16] != [0; 6]
        || header[24..28] != [0; 4]
        || id == 0
        || crc32fast::hash(&header[..28]) != u32::from_le_bytes(crc)
    {
        return Err(invalid());
    }
    Ok(Segment {
        id,
        size,
        durable: size,
    })
}
