//! Two CRC-protected slots per stream. Space is physically reserved at startup.
use crate::capture::{registry::Registry, state::State};
use apollo_telemetry_protocol::{Descriptor, Gap, invalid, local::private_file};
use std::{fs::File, io, os::unix::fs::FileExt, path::Path};
const HEADER: u64 = 64;
const SLOT: usize = 256;

pub struct Checkpoint {
    file: File,
    pub bytes: u64,
}
impl Checkpoint {
    pub fn open(path: &Path, registry: &mut Registry) -> io::Result<Self> {
        let file = private_file(path)?;
        let bytes = HEADER + (registry.capacity * SLOT * 2) as u64;
        if file.metadata()?.len() == 0 {
            rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::empty(), 0, bytes)?;
            let mut header = [0; 64];
            header[..8].copy_from_slice(b"ALSTATE1");
            header[8..16].copy_from_slice(&(registry.capacity as u64).to_le_bytes());
            let crc = crc32fast::hash(&header[..60]);
            header[60..].copy_from_slice(&crc.to_le_bytes());
            file.write_all_at(&header, 0)?;
            file.sync_data()?;
        }
        if file.metadata()?.len() != bytes {
            return Err(invalid());
        }
        let mut header = [0; 64];
        file.read_exact_at(&mut header, 0)?;
        if header[..8] != *b"ALSTATE1"
            || read64(&header, 8) != registry.capacity as u64
            || header[16..60].iter().any(|&b| b != 0)
            || crc32fast::hash(&header[..60]) != read32(&header, 60)
        {
            return Err(invalid());
        }
        let mut empty_seen = false;
        for index in 0..registry.capacity {
            let mut slots = [[0; SLOT]; 2];
            for (copy, slot) in slots.iter_mut().enumerate() {
                file.read_exact_at(slot, offset(index, copy))?;
            }
            let a = decode(&slots[0]);
            let b = decode(&slots[1]);
            let state = match (a, b) {
                (Ok(Some(a)), Ok(Some(b))) => Some(if a.version > b.version { a } else { b }),
                (Ok(Some(a)), _) | (_, Ok(Some(a))) => Some(a),
                (Ok(None), Ok(None)) => None,
                _ => return Err(invalid()),
            };
            if let Some(state) = state {
                if empty_seen
                    || registry
                        .states
                        .iter()
                        .any(|s| s.descriptor.stream == state.descriptor.stream)
                {
                    return Err(invalid());
                }
                registry.states.push(state);
            } else {
                empty_seen = true;
            }
        }
        Ok(Self { file, bytes })
    }
    /// Caller syncs WAL first. A failed checkpoint must never authorize delivery.
    pub fn save(&mut self, registry: &mut Registry) -> io::Result<()> {
        for (index, state) in registry.states.iter_mut().enumerate() {
            if !state.dirty {
                continue;
            }
            state.version = state.version.checked_add(1).ok_or_else(invalid)?;
            self.file
                .write_all_at(&encode(state), offset(index, (state.version % 2) as usize))?;
        }
        self.file.sync_data()?;
        for state in &mut registry.states {
            state.dirty = false;
            state.durable = state.sequence;
        }
        Ok(())
    }
}
fn offset(index: usize, copy: usize) -> u64 {
    HEADER + ((index * 2 + copy) * SLOT) as u64
}
fn encode(state: &State) -> [u8; SLOT] {
    let mut out = [0; SLOT];
    out[..4].copy_from_slice(b"ALS1");
    put64(&mut out, 8, state.version);
    out[16..64].copy_from_slice(&state.descriptor.encode());
    put64(&mut out, 64, state.sequence);
    put64(&mut out, 72, state.acknowledged);
    out[80] = u8::from(state.closing) | (u8::from(state.closed) << 1);
    put64(&mut out, 88, state.close_sequence);
    out[96..136].copy_from_slice(&state.frozen.encode());
    out[136..176].copy_from_slice(&state.pending.encode());
    put64(&mut out, 176, state.gap_wall);
    put64(&mut out, 184, state.gap_mono);
    let crc = crc32fast::hash(&out[..252]);
    out[252..].copy_from_slice(&crc.to_le_bytes());
    out
}
fn decode(bytes: &[u8; SLOT]) -> io::Result<Option<State>> {
    if bytes.iter().all(|&b| b == 0) {
        return Ok(None);
    }
    if bytes[..4] != *b"ALS1"
        || bytes[4..8] != [0; 4]
        || crc32fast::hash(&bytes[..252]) != read32(bytes, 252)
        || bytes[81..88] != [0; 7]
        || bytes[192..252].iter().any(|&b| b != 0)
        || bytes[80] > 3
    {
        return Err(invalid());
    }
    let mut state = State::new(Descriptor::decode(&bytes[16..64])?);
    state.version = read64(bytes, 8);
    state.sequence = read64(bytes, 64);
    state.acknowledged = read64(bytes, 72);
    state.closing = bytes[80] & 1 != 0;
    state.closed = bytes[80] & 2 != 0;
    state.close_sequence = read64(bytes, 88);
    state.frozen = gap(&bytes[96..136])?;
    state.pending = gap(&bytes[136..176])?;
    state.gap_wall = read64(bytes, 176);
    state.gap_mono = read64(bytes, 184);
    state.durable = state.sequence;
    state.dirty = false;
    if state.version == 0
        || state.acknowledged > state.sequence
        || state.close_sequence > state.sequence
        || (state.closed && (!state.closing || state.close_sequence == 0))
        || state.frozen.last > state.sequence
        || state.pending.last > state.sequence
        || (state.pending.first != 0 && state.frozen.last >= state.pending.first)
    {
        return Err(invalid());
    }
    Ok(Some(state))
}
fn gap(bytes: &[u8]) -> io::Result<Gap> {
    if bytes.iter().all(|&b| b == 0) {
        Ok(Gap::default())
    } else {
        Gap::decode(bytes)
    }
}
fn put64(out: &mut [u8], at: usize, value: u64) {
    out[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
fn read64(bytes: &[u8], at: usize) -> u64 {
    let mut v = [0; 8];
    v.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(v)
}
fn read32(bytes: &[u8], at: usize) -> u32 {
    let mut v = [0; 4];
    v.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(v)
}
