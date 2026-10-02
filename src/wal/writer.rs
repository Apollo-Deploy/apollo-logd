use super::{
    checkpoint::Checkpoint,
    recovery,
    segment::{self, Segment},
};
use crate::capture::registry::StateRef;
use crate::{capture::registry::Registry, config::Config};
use apollo_telemetry_protocol::{
    invalid,
    local::{private_file, safe_directory},
    record,
};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::FileExt,
    path::PathBuf,
};

pub struct Wal {
    pub root: PathBuf,
    pub segments: Vec<Segment>,
    pub bytes: u64,
    file: File,
    checkpoint: Checkpoint,
    _lock: File,
    quota: u64,
    limit: u64,
    reserve: u64,
}
impl Wal {
    pub fn open(config: &Config, registry: &mut Registry) -> io::Result<Self> {
        safe_directory(&config.spool)?;
        let lock = private_file(&config.spool.join("daemon.lock"))?;
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
        let checkpoint = Checkpoint::open(&config.spool.join("state"), registry)?;
        File::open(&config.spool)?.sync_all()?;
        let mut segments = recovery::discover(
            &config.spool,
            config.segment,
            config.quota - checkpoint.bytes,
        )?;
        recovery::scan(&config.spool, &mut segments, registry)?;
        let file = if let Some(last) = segments.last() {
            private_file(&segment::path(&config.spool, last.id))?
        } else {
            let (file, segment) = segment::create(&config.spool, 1)?;
            segments.push(segment);
            file
        };
        let bytes = segments.iter().map(|s| s.size).sum::<u64>() + checkpoint.bytes;
        Ok(Self {
            root: config.spool.clone(),
            segments,
            bytes,
            file,
            checkpoint,
            _lock: lock,
            quota: config.quota,
            limit: config.segment,
            reserve: config.reserve,
        })
    }
    /// Returns false on quota/ENOSPC, without leaving an ambiguous partial record.
    pub fn append(&mut self, record: &[u8]) -> io::Result<bool> {
        let length = record.len() as u64;
        let extra = if self.segments.last().ok_or_else(invalid)?.size + length > self.limit {
            segment::HEADER
        } else {
            0
        };
        let fs = rustix::fs::fstatvfs(&self.file)?;
        let available = fs.f_bavail.saturating_mul(fs.f_frsize);
        if self.bytes + length + extra > self.quota
            || available < self.reserve.saturating_add(length + extra)
            || (extra != 0 && self.segments.len() >= 8192)
        {
            return Ok(false);
        }
        if extra != 0
            && let Err(error) = self.roll()
        {
            if matches!(error.raw_os_error(), Some(28) | Some(122)) {
                return Ok(false);
            }
            return Err(error);
        }
        let segment = self.segments.last_mut().ok_or_else(invalid)?;
        let before = segment.size;
        if let Err(error) = self.file.write_all_at(record, before) {
            self.file.set_len(before)?;
            if matches!(error.raw_os_error(), Some(28) | Some(122)) {
                return Ok(false);
            }
            return Err(error);
        }
        segment.size += length;
        self.bytes += length;
        Ok(true)
    }
    pub fn sync(&mut self, registry: &mut Registry) -> io::Result<()> {
        self.file.sync_data()?;
        for state in &mut registry.states {
            state.freeze();
        }
        self.checkpoint.save(registry)?;
        if let Some(last) = self.segments.last_mut() {
            last.durable = last.size;
        }
        Ok(())
    }
    pub fn roll(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        if let Some(last) = self.segments.last_mut() {
            last.durable = last.size;
        }
        if self.segments.len() >= 8192 {
            return Err(invalid());
        }
        let id = self
            .segments
            .last()
            .ok_or_else(invalid)?
            .id
            .checked_add(1)
            .ok_or_else(invalid)?;
        let (file, segment) = segment::create(&self.root, id)?;
        self.file = file;
        self.segments.push(segment);
        self.bytes += segment::HEADER;
        Ok(())
    }
    pub fn reclaim(&mut self, registry: &mut Registry, scratch: &mut [u8]) -> io::Result<()> {
        while self.segments[0].size > segment::HEADER {
            let segment = self.segments[0];
            let file = private_file(&segment::path(&self.root, segment.id))?;
            let mut offset = segment::HEADER;
            let mut covered = true;
            while offset < segment.size {
                file.read_exact_at(&mut scratch[..8], offset)?;
                let length = apollo_telemetry_protocol::record::length(&scratch[..8])?;
                file.read_exact_at(&mut scratch[..length], offset)?;
                let (header, _) = apollo_telemetry_protocol::record::decode(&scratch[..length])?;
                let reference = registry.find(header.stream)?;
                let index = registry.resolve(reference)?;
                if registry.states[index].descriptor.generation != header.generation {
                    return Err(invalid());
                }
                if registry.states[index].acknowledged < header.sequence {
                    covered = false;
                    break;
                }
                offset += length as u64;
            }
            if !covered {
                break;
            }
            // ACK checkpoint was fdatasynced by sync() before deleting segments.
            offset = segment::HEADER;
            while offset < segment.size {
                file.read_exact_at(&mut scratch[..8], offset)?;
                let length = apollo_telemetry_protocol::record::length(&scratch[..8])?;
                file.read_exact_at(&mut scratch[..length], offset)?;
                let (header, _) = apollo_telemetry_protocol::record::decode(&scratch[..length])?;
                let reference = registry.find(header.stream)?;
                let index = registry.resolve(reference)?;
                if registry.states[index].descriptor.generation != header.generation {
                    return Err(invalid());
                }
                registry.states[index].retained = registry.states[index]
                    .retained
                    .saturating_sub(length as u64);
                offset += length as u64;
            }
            let replace_active = self.segments.len() == 1;
            drop(file);
            if replace_active {
                // Close before unlink so acknowledged blocks are released even
                // on ENOSPC. Directory FD is a temporary ownership placeholder;
                // no append can run concurrently in this synchronous reactor.
                self.file = File::open(&self.root)?;
            }
            fs::remove_file(segment::path(&self.root, segment.id))?;
            File::open(&self.root)?.sync_all()?;
            self.bytes -= segment.size;
            self.segments.remove(0);
            if replace_active {
                let id = segment.id.checked_add(1).ok_or_else(invalid)?;
                let (file, next) = segment::create(&self.root, id)?;
                self.file = file;
                self.segments.push(next);
                self.bytes += segment::HEADER;
            }
        }
        Ok(())
    }

    /// Check the storage prerequisites for retiring a stream. Every retained
    /// WAL segment is scanned because acknowledged records can remain behind a
    /// different stream that still pins an older segment. This deliberately
    /// does not establish authority to retire or reuse the identity.
    #[allow(dead_code)] // Staged until the Agent authority-head contract exists.
    pub fn storage_retirement_ready(
        &self,
        registry: &Registry,
        reference: StateRef,
        scratch: &mut [u8],
    ) -> io::Result<bool> {
        if scratch.len() < record::HEADER + apollo_telemetry_protocol::MAX_PAYLOAD
            || !registry.acknowledged_closed(reference)?
        {
            return Ok(false);
        }
        let slot = registry.resolve(reference)?;
        let descriptor = registry.states[slot].descriptor;
        for segment in &self.segments {
            let file = private_file(&segment::path(&self.root, segment.id))?;
            let mut offset = segment::HEADER;
            while offset < segment.size {
                if segment.size - offset < 8 {
                    return Err(invalid());
                }
                file.read_exact_at(&mut scratch[..8], offset)?;
                let length = record::length(&scratch[..8])?;
                if offset + length as u64 > segment.size {
                    return Err(invalid());
                }
                file.read_exact_at(&mut scratch[..length], offset)?;
                let (header, _) = record::decode(&scratch[..length])?;
                if header.stream == descriptor.stream {
                    if header.generation != descriptor.generation {
                        return Err(invalid());
                    }
                    return Ok(false);
                }
                offset += length as u64;
            }
        }
        Ok(true)
    }
}
