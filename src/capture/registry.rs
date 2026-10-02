use super::state::State;
use apollo_telemetry_protocol::{Descriptor, StreamId, invalid};
use std::io;

/// A checked reference to one immutable stream identity in the append-only
/// registry. The slot is only a lookup hint; every resolution also checks the
/// stream and generation so a future slot reuse cannot retarget stale work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateRef {
    slot: usize,
    descriptor: Descriptor,
}
impl StateRef {
    pub fn slot(self) -> usize {
        self.slot
    }
}

pub struct Registry {
    pub states: Vec<State>,
    pub capacity: usize,
}
impl Registry {
    pub fn new(capacity: usize) -> Self {
        Self {
            states: Vec::with_capacity(capacity),
            capacity,
        }
    }
    pub fn find(&self, id: StreamId) -> io::Result<StateRef> {
        let slot = self
            .states
            .iter()
            .position(|s| s.descriptor.stream == id)
            .ok_or_else(invalid)?;
        Ok(self.reference(slot))
    }
    pub fn reference(&self, slot: usize) -> StateRef {
        let state = &self.states[slot];
        StateRef {
            slot,
            descriptor: state.descriptor,
        }
    }
    pub fn resolve(&self, reference: StateRef) -> io::Result<usize> {
        self.states
            .get(reference.slot)
            .filter(|state| state.descriptor == reference.descriptor)
            .map(|_| reference.slot)
            .ok_or_else(invalid)
    }
    /// Whether sequence state is durably closed and acknowledged. This is one
    /// prerequisite for retirement, not authority to retire or reuse a stream.
    pub fn acknowledged_closed(&self, reference: StateRef) -> io::Result<bool> {
        let state = &self.states[self.resolve(reference)?];
        Ok(state.closing
            && state.closed
            && state.close_sequence != 0
            && state.sequence == state.close_sequence
            && state.durable >= state.close_sequence
            && state.acknowledged >= state.close_sequence
            && !state.dirty
            && state.frozen.first == 0
            && state.pending.first == 0)
    }
    pub fn register(&mut self, descriptor: Descriptor) -> io::Result<StateRef> {
        if self.states.iter().any(|s| {
            s.descriptor.workload == descriptor.workload
                && s.descriptor.generation > descriptor.generation
        }) {
            return Err(invalid());
        }
        if let Ok(reference) = self.find(descriptor.stream) {
            let index = self.resolve(reference)?;
            let state = &self.states[index];
            if state.descriptor != descriptor || state.closing || state.closed {
                return Err(invalid());
            }
            return Ok(reference);
        }
        if self.states.len() == self.capacity
            || self.states.iter().any(|s| {
                s.descriptor.workload == descriptor.workload
                    && s.descriptor.generation > descriptor.generation
            })
        {
            return Err(invalid());
        }
        self.states.push(State::new(descriptor));
        Ok(self.reference(self.states.len() - 1))
    }
    pub fn ack(&mut self, id: StreamId, generation: u64, through: u64) -> io::Result<()> {
        let index = self.resolve(self.find(id)?)?;
        let state = &mut self.states[index];
        if (state.frozen.first != 0 && through >= state.frozen.first && through < state.frozen.last)
            || generation != state.descriptor.generation
            || through > state.sent
            || through > state.durable
        {
            return Err(invalid());
        }
        if through > state.acknowledged {
            state.acknowledged = through;
            state.dirty = true;
            if state.frozen.first != 0 && through >= state.frozen.last {
                state.frozen = Default::default();
            }
        }
        Ok(())
    }
}
