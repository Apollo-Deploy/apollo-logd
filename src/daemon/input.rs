use super::*;
use crate::capture::registry::StateRef;
use apollo_telemetry_protocol::{
    Channel, Gap, invalid,
    packet::{self, Kind},
    record::RecordKind,
};
// Three full capture chunks fit in one 128 KiB protocol packet. The protocol
// still accepts 64 KiB payloads from external producers and splits them here.
const CAPTURE_CHUNK: usize = 32 * 1024;
impl Engine {
    /// Move reserved loss metadata into the WAL whenever capacity permits. This
    /// lets capture resume even while Agent is offline. Sync WAL before saving
    /// the cleared checkpoint slots; recovery reconciles an interrupted move.
    pub(super) fn persist_gaps(&mut self) -> io::Result<()> {
        for state in &mut self.registry.states {
            for _ in 0..2 {
                state.freeze();
                if state.frozen.first == 0 {
                    break;
                }
                let mut header = state.header(RecordKind::Gap, Channel::Control, state.frozen.last);
                header.realtime_ns = state.gap_wall;
                header.monotonic_ns = state.gap_mono;
                record::encode(header, &state.frozen.encode(), &mut self.encoded)?;
                if !self.wal.append(&self.encoded)? {
                    break;
                }
                state.retained += self.encoded.len() as u64;
                state.frozen = Gap::default();
                state.dirty = true;
            }
        }
        Ok(())
    }
    pub(super) fn capture(&mut self, index: usize) -> io::Result<()> {
        if index >= self.sources.len() {
            return Err(invalid());
        }
        for _ in 0..4 {
            let Some(fd) = &self.sources[index] else {
                return Ok(());
            };
            match rustix::io::read(fd, &mut self.input[..CAPTURE_CHUNK]) {
                Ok(0) => {
                    let _ = epoll::delete(&self.epoll, fd);
                    self.sources[index] = None;
                    let stream = self.registry.reference(index / 2);
                    let slot = stream.slot();
                    if self.sources[slot * 2..slot * 2 + 2]
                        .iter()
                        .all(Option::is_none)
                    {
                        let slot = self.registry.resolve(stream)?;
                        self.registry.states[slot].closing = true;
                        self.registry.states[slot].dirty = true;
                    }
                    return Ok(());
                }
                Ok(n) => {
                    let stream = self.registry.reference(index / 2);
                    self.append_input(
                        stream,
                        if index.is_multiple_of(2) {
                            Channel::Stdout
                        } else {
                            Channel::Stderr
                        },
                        0,
                        n,
                    )?;
                }
                Err(rustix::io::Errno::AGAIN) => break,
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    pub(super) fn feed(&mut self, stream: StateRef, kind: Kind, length: usize) -> io::Result<()> {
        let body = &self.input[packet::HEADER..length];
        let slot = self.registry.resolve(stream)?;
        if self.registry.states[slot].closing {
            return Err(invalid());
        }
        match kind {
            Kind::Data => {
                if !(9..=MAX_PAYLOAD + 8).contains(&body.len()) || body[1..8] != [0; 7] {
                    return Err(invalid());
                }
                let channel = Channel::try_from(body[0])?;
                if channel == Channel::Control {
                    return Err(invalid());
                }
                let payload_len = body.len() - 8;
                for offset in (0..payload_len).step_by(CAPTURE_CHUNK) {
                    self.append_input(
                        stream,
                        channel,
                        packet::HEADER + 8 + offset,
                        CAPTURE_CHUNK.min(payload_len - offset),
                    )?;
                }
            }
            Kind::Lost => {
                let gap = Gap::decode(body)?;
                if gap.reason != 4 || gap.records == 0 || gap.records > u32::MAX as u64 {
                    return Err(invalid());
                }
                let slot = self.registry.resolve(stream)?;
                self.registry.states[slot].loss(gap.bytes, gap.records, 4)?;
                self.counters[4] = self.counters[4].saturating_add(gap.bytes);
                self.counters[5] = self.counters[5].saturating_add(1);
            }
            Kind::Close if body.is_empty() => {
                let slot = self.registry.resolve(stream)?;
                self.registry.states[slot].closing = true;
                self.registry.states[slot].dirty = true;
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
    fn append_input(
        &mut self,
        stream: StateRef,
        channel: Channel,
        offset: usize,
        length: usize,
    ) -> io::Result<()> {
        let slot = self.registry.resolve(stream)?;
        let descriptor = self.registry.states[slot].descriptor;
        let stale = self.registry.states.iter().any(|s| {
            s.descriptor.workload == descriptor.workload
                && s.descriptor.generation > descriptor.generation
        });
        let state = &mut self.registry.states[slot];
        if state.closed {
            return Err(invalid());
        }
        let now = crate::capture::state::timestamps().1 / 1_000_000_000;
        if state.rate_epoch != now {
            state.rate_epoch = now;
            state.rate_used = 0;
        }
        state.rate_used = state.rate_used.saturating_add(length as u64);
        let quota = self.config.stream_quota != 0
            && state.retained + length as u64 > self.config.stream_quota;
        let rate = self.config.rate != 0 && state.rate_used > self.config.rate;
        let dropping =
            stale || quota || rate || state.frozen.first != 0 || state.pending.first != 0;
        let sequence = state.sequence.checked_add(1).ok_or_else(invalid)?;
        let mut header = state.header(RecordKind::Bytes, channel, sequence);
        if length == CAPTURE_CHUNK {
            header.flags = record::CONTINUATION;
        }
        record::encode(
            header,
            &self.input[offset..offset + length],
            &mut self.encoded,
        )?;
        self.counters[1] = self.counters[1].saturating_add(length as u64);
        if !dropping && self.wal.append(&self.encoded)? {
            state.sequence = sequence;
            state.retained += self.encoded.len() as u64;
            state.dirty = true;
        } else {
            state.loss(
                length as u64,
                1,
                if rate {
                    2
                } else if stale {
                    3
                } else {
                    1
                },
            )?;
            self.counters[4] = self.counters[4].saturating_add(length as u64);
            self.counters[5] = self.counters[5].saturating_add(1);
        }
        Ok(())
    }
    pub(super) fn finish_closed(&mut self) -> io::Result<()> {
        for (index, state) in self.registry.states.iter_mut().enumerate() {
            if !state.closing
                || state.closed
                || state.frozen.first != 0
                || state.pending.first != 0
                || self.sources[index * 2..index * 2 + 2]
                    .iter()
                    .any(Option::is_some)
            {
                continue;
            }
            let sequence = state.sequence.checked_add(1).ok_or_else(invalid)?;
            record::encode(
                state.header(RecordKind::Close, Channel::Control, sequence),
                &[],
                &mut self.encoded,
            )?;
            if self.wal.append(&self.encoded)? {
                state.sequence = sequence;
                state.close_sequence = sequence;
                state.closed = true;
                state.retained += self.encoded.len() as u64;
                state.dirty = true;
            }
        }
        Ok(())
    }
}
