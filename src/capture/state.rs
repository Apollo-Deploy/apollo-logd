use apollo_telemetry_protocol::{
    Channel, Descriptor, Gap,
    record::{Header, RecordKind},
};

pub struct State {
    pub descriptor: Descriptor,
    pub sequence: u64,
    pub acknowledged: u64,
    pub sent: u64,
    pub frozen: Gap,
    pub pending: Gap,
    pub gap_wall: u64,
    pub gap_mono: u64,
    pub closing: bool,
    pub closed: bool,
    pub close_sequence: u64,
    pub version: u64,
    pub dirty: bool,
    pub durable: u64,
    pub retained: u64,
    pub rate_epoch: u64,
    pub rate_used: u64,
}
impl State {
    pub fn new(descriptor: Descriptor) -> Self {
        Self {
            descriptor,
            sequence: 0,
            acknowledged: 0,
            sent: 0,
            frozen: Gap::default(),
            pending: Gap::default(),
            gap_wall: 0,
            gap_mono: 0,
            closing: false,
            closed: false,
            close_sequence: 0,
            version: 0,
            dirty: true,
            durable: 0,
            retained: 0,
            rate_epoch: 0,
            rate_used: 0,
        }
    }
    pub fn header(&self, kind: RecordKind, channel: Channel, sequence: u64) -> Header {
        let (wall, mono) = timestamps();
        Header {
            kind,
            channel,
            flags: 0,
            stream: self.descriptor.stream,
            generation: self.descriptor.generation,
            sequence,
            realtime_ns: wall,
            monotonic_ns: mono,
        }
    }
    pub fn loss(&mut self, bytes: u64, records: u64, reason: u32) -> std::io::Result<()> {
        let first = self
            .sequence
            .checked_add(1)
            .ok_or_else(apollo_telemetry_protocol::invalid)?;
        self.sequence = self
            .sequence
            .checked_add(records)
            .ok_or_else(apollo_telemetry_protocol::invalid)?;
        if self.pending.first == 0 {
            self.pending.first = first;
            self.pending.reason = reason;
        }
        self.pending.last = self.sequence;
        self.pending.bytes = self.pending.bytes.saturating_add(bytes);
        self.pending.records = self.pending.records.saturating_add(records);
        self.dirty = true;
        Ok(())
    }
    pub fn freeze(&mut self) {
        if self.frozen.first == 0 && self.pending.first != 0 {
            self.frozen = self.pending;
            self.pending = Gap::default();
            (self.gap_wall, self.gap_mono) = timestamps();
            self.dirty = true;
        }
    }
}
pub fn timestamps() -> (u64, u64) {
    fn ns(id: rustix::time::ClockId) -> u64 {
        let time = rustix::time::clock_gettime(id);
        (time.tv_sec.max(0) as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(time.tv_nsec.max(0) as u64)
    }
    (
        ns(rustix::time::ClockId::Realtime),
        ns(rustix::time::ClockId::Monotonic),
    )
}
