use super::*;
use apollo_telemetry_protocol::{
    Channel, Gap, invalid,
    packet::{self, Kind},
    record::RecordKind,
};
impl Engine {
    pub(super) fn deliver(&mut self) -> io::Result<()> {
        let Some(agent) = self.agent else {
            return Ok(());
        };
        for _ in 0..8 {
            if !self.pending {
                while self.registration < self.registry.states.len() {
                    let state = &self.registry.states[self.registration];
                    if !state.closed || state.acknowledged < state.close_sequence {
                        break;
                    }
                    self.registration += 1;
                }
                if self.registration < self.registry.states.len() {
                    let state = &self.registry.states[self.registration];
                    packet::encode(Kind::Register, &state.descriptor.encode(), &mut self.packet)?;
                } else {
                    self.make_batch()?;
                    if self.batch.is_empty() {
                        if self.last_counters.elapsed() < Duration::from_secs(1) {
                            self.interest(agent, false)?;
                            return Ok(());
                        }
                        self.counters[0] =
                            self.registry.states.iter().filter(|s| !s.closed).count() as u64;
                        self.counters[2] = self.wal.bytes;
                        self.counters[3] = self.registry.states.iter().fold(0u64, |sum, s| {
                            sum.saturating_add(s.sequence - s.acknowledged)
                        });
                        self.counters[6] = 1;
                        let mut body = [0; 72];
                        for (value, slot) in self.counters.iter().zip(body.chunks_exact_mut(8)) {
                            slot.copy_from_slice(&value.to_le_bytes());
                        }
                        packet::encode(Kind::Counters, &body, &mut self.packet)?;
                        self.last_counters = Instant::now();
                    } else {
                        packet::encode(Kind::Batch, &self.batch, &mut self.packet)?;
                    }
                }
                self.pending = true;
            }
            let connection = self.connections[agent].as_ref().ok_or_else(invalid)?;
            match local::send(&connection.fd, &self.packet, &[]) {
                Ok(()) => {
                    self.pending = false;
                    let (kind, mut body) = packet::decode(&self.packet)?;
                    if kind == Kind::Register {
                        self.registration += 1;
                    } else if kind == Kind::Batch {
                        while !body.is_empty() {
                            let n = record::length(body)?;
                            let (header, payload) = record::decode(&body[..n])?;
                            let reference = self.registry.find(header.stream)?;
                            let slot = self.registry.resolve(reference)?;
                            if self.registry.states[slot].descriptor.generation != header.generation
                            {
                                return Err(invalid());
                            }
                            if header.kind == RecordKind::Gap {
                                let gap = Gap::decode(payload)?;
                                self.sent_gaps.push((reference, gap.first, gap.last));
                            }
                            self.registry.states[slot].sent = header.sequence;
                            self.counters[7] = self.counters[7].saturating_add(1);
                            body = &body[n..];
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.interest(agent, true)?;
                    return Ok(());
                }
                Err(_) => {
                    self.disconnect(agent)?;
                    return Ok(());
                }
            }
        }
        self.interest(agent, true)
    }
    fn interest(&self, index: usize, write: bool) -> io::Result<()> {
        if let Some(connection) = &self.connections[index] {
            let flags = EventFlags::IN
                | if write {
                    EventFlags::OUT
                } else {
                    EventFlags::empty()
                };
            epoll::modify(
                &self.epoll,
                &connection.fd,
                EventData::new_u64(CONNECTION + index as u64),
                flags,
            )?;
        }
        Ok(())
    }
    fn make_batch(&mut self) -> io::Result<()> {
        self.batch.clear();
        let limit = MAX_PACKET - packet::HEADER;
        let mut through: Vec<_> = self.registry.states.iter().map(|s| s.sent).collect();
        let mut gaps_left = 4096 - self.sent_gaps.len();
        for _ in 0..2048 {
            let before = self.cursor;
            let Some(length) = self.cursor.next(&self.wal, &mut self.scratch)? else {
                break;
            };
            let (header, payload) = record::decode(&self.scratch[..length])?;
            let first = if header.kind == RecordKind::Gap {
                Gap::decode(payload)?.first
            } else {
                header.sequence
            };
            let reference = self.registry.find(header.stream)?;
            let index = reference.slot();
            if self.registry.states[self.registry.resolve(reference)?]
                .descriptor
                .generation
                != header.generation
            {
                return Err(invalid());
            }
            if header.sequence <= through[index] {
                continue;
            }
            let state = &self.registry.states[index];
            // Rotation syncs a completed file before the stream checkpoint.
            // Delivery must wait for both; otherwise a valid immediate ACK
            // could be ahead of the checkpoint's durable sequence frontier.
            if header.sequence > state.durable {
                self.cursor = before;
                break;
            }
            if first > through[index].saturating_add(1)
                && state.frozen.first == through[index].saturating_add(1)
                && state.frozen.last < first
            {
                let mut gap_header =
                    state.header(RecordKind::Gap, Channel::Control, state.frozen.last);
                gap_header.realtime_ns = state.gap_wall;
                gap_header.monotonic_ns = state.gap_mono;
                record::encode(gap_header, &state.frozen.encode(), &mut self.encoded)?;
                if gaps_left == 0 || self.batch.len() + self.encoded.len() > limit {
                    self.cursor = before;
                    break;
                }
                self.batch.extend_from_slice(&self.encoded);
                gaps_left -= 1;
                through[index] = state.frozen.last;
            }
            if (header.kind == RecordKind::Gap && gaps_left == 0)
                || self.batch.len() + length > limit
            {
                self.cursor = before;
                break;
            }
            if first != through[index].checked_add(1).ok_or_else(invalid)? {
                return Err(invalid());
            }
            self.batch.extend_from_slice(&self.scratch[..length]);
            if header.kind == RecordKind::Gap {
                gaps_left -= 1;
            }
            through[index] = header.sequence;
        }
        for (index, state) in self.registry.states.iter().enumerate() {
            if state.frozen.first == 0
                || state.frozen.first != through[index].saturating_add(1)
                || state.frozen.last > state.durable
            {
                continue;
            }
            let mut header = state.header(RecordKind::Gap, Channel::Control, state.frozen.last);
            header.realtime_ns = state.gap_wall;
            header.monotonic_ns = state.gap_mono;
            record::encode(header, &state.frozen.encode(), &mut self.encoded)?;
            if gaps_left == 0 || self.batch.len() + self.encoded.len() > limit {
                break;
            }
            self.batch.extend_from_slice(&self.encoded);
            gaps_left -= 1;
            through[index] = state.frozen.last;
        }
        Ok(())
    }
}
