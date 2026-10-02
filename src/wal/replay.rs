use super::{Wal, segment};
use apollo_telemetry_protocol::{invalid, record};
use std::{fs::File, io, os::unix::fs::FileExt};

#[derive(Default, Clone, Copy)]
pub struct Cursor {
    pub id: u64,
    pub offset: u64,
}
impl Cursor {
    pub fn next(&mut self, wal: &Wal, scratch: &mut [u8]) -> io::Result<Option<usize>> {
        for segment in &wal.segments {
            if segment.id < self.id {
                continue;
            }
            if segment.id != self.id {
                self.id = segment.id;
                self.offset = segment::HEADER;
            }
            if self.offset >= segment.durable {
                continue;
            }
            let file = File::open(segment::path(&wal.root, segment.id))?;
            file.read_exact_at(&mut scratch[..8], self.offset)?;
            let length = record::length(&scratch[..8])?;
            if self.offset + length as u64 > segment.durable {
                return Err(invalid());
            }
            file.read_exact_at(&mut scratch[..length], self.offset)?;
            record::decode(&scratch[..length])?;
            self.offset += length as u64;
            return Ok(Some(length));
        }
        Ok(None)
    }
}
