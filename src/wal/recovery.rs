use super::segment::{self, Segment};
use crate::capture::registry::Registry;
use apollo_telemetry_protocol::{
    Gap, MAX_PAYLOAD, invalid,
    local::private_file,
    record::{self, RecordKind},
};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::FileExt,
    path::Path,
};

pub fn discover(root: &Path, max_size: u64, quota: u64) -> io::Result<Vec<Segment>> {
    let mut segments = Vec::new();
    let mut total = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(invalid)?;
        if name == "segment-new" {
            // Only an unpublished header lives here; records are appended after
            // atomic rename and directory sync. No customer bytes are removed.
            private_file(&entry.path())?;
            fs::remove_file(entry.path())?;
            File::open(root)?.sync_all()?;
            continue;
        }
        if name == "state" || name == "daemon.lock" {
            continue;
        }
        if !name.starts_with("segment-")
            || !name.ends_with(".wal")
            || !entry.file_type()?.is_file()
            || segments.len() >= 8192
        {
            return Err(invalid());
        }
        let file = private_file(&entry.path())?;
        let segment = segment::inspect(&file)?;
        if segment.size > max_size || entry.path() != segment::path(root, segment.id) {
            return Err(invalid());
        }
        total = total.checked_add(segment.size).ok_or_else(invalid)?;
        if total > quota {
            return Err(invalid());
        }
        segments.push(segment);
    }
    segments.sort_unstable_by_key(|s| s.id);
    if segments
        .windows(2)
        .any(|s| s[0].id.checked_add(1) != Some(s[1].id))
    {
        return Err(invalid());
    }
    Ok(segments)
}

/// Only an incomplete final record may be truncated. Full CRC corruption is fatal.
pub fn scan(root: &Path, segments: &mut [Segment], registry: &mut Registry) -> io::Result<()> {
    let mut buffer = vec![0; record::HEADER + MAX_PAYLOAD];
    let final_id = segments.last().map(|s| s.id);
    let mut known: Vec<u64> = registry.states.iter().map(|s| s.acknowledged).collect();
    for segment in segments {
        let file = private_file(&segment::path(root, segment.id))?;
        let mut offset = segment::HEADER;
        while offset < segment.size {
            let remaining = segment.size - offset;
            if remaining < 8 {
                truncate(&file, segment, offset, final_id)?;
                break;
            }
            file.read_exact_at(&mut buffer[..8], offset)?;
            let length = record::length(&buffer[..8])?;
            if remaining < (length as u64) {
                truncate(&file, segment, offset, final_id)?;
                break;
            }
            file.read_exact_at(&mut buffer[..length], offset)?;
            let (header, payload) = record::decode(&buffer[..length])?;
            let reference = registry.find(header.stream)?;
            let index = registry.resolve(reference)?;
            let state = &mut registry.states[index];
            if state.descriptor.generation != header.generation
                || (header.sequence > state.acknowledged && header.sequence <= known[index])
            {
                return Err(invalid());
            }
            let first = if header.kind == RecordKind::Gap {
                let gap = Gap::decode(payload)?;
                if state.frozen.first == gap.first {
                    if state.frozen != gap
                        || state.gap_wall != header.realtime_ns
                        || state.gap_mono != header.monotonic_ns
                    {
                        return Err(invalid());
                    }
                    state.frozen = Gap::default();
                    state.dirty = true;
                }
                if state.pending.first == gap.first && state.pending.last <= gap.last {
                    state.pending = Gap::default();
                    state.dirty = true;
                }
                gap.first
            } else {
                header.sequence
            };
            if header.sequence > state.acknowledged
                && first != known[index].checked_add(1).ok_or_else(invalid)?
            {
                return Err(invalid());
            }
            known[index] = known[index].max(header.sequence);
            state.retained += length as u64;
            if state.sequence < header.sequence {
                state.sequence = header.sequence;
                state.dirty = true;
            }
            if header.kind == RecordKind::Close {
                state.closing = true;
                state.closed = true;
                state.close_sequence = header.sequence;
                state.dirty = true;
            }
            offset += length as u64;
        }
    }
    for (index, state) in registry.states.iter_mut().enumerate() {
        let covered = known[index].max(state.frozen.last).max(state.pending.last);
        if covered < state.sequence {
            // A checkpointed complete record cannot vanish after fdatasync.
            // This indicates storage corruption, not a recoverable short append.
            return Err(invalid());
        }
        state.durable = state.sequence;
    }
    Ok(())
}
fn truncate(file: &File, segment: &mut Segment, offset: u64, last: Option<u64>) -> io::Result<()> {
    if Some(segment.id) != last {
        return Err(invalid());
    }
    file.set_len(offset)?;
    file.sync_data()?;
    segment.size = offset;
    segment.durable = offset;
    Ok(())
}
