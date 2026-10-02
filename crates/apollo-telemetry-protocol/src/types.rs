use crate::{array, invalid};
use std::io;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct StreamId(pub [u8; 16]);
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkloadId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SourceKind {
    Runtime = 1,
    Build = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Channel {
    Control = 0,
    Stdout = 1,
    Stderr = 2,
}

impl TryFrom<u8> for Channel {
    type Error = io::Error;
    fn try_from(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Control),
            1 => Ok(Self::Stdout),
            2 => Ok(Self::Stderr),
            _ => Err(invalid()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Descriptor {
    pub stream: StreamId,
    pub workload: WorkloadId,
    pub generation: u64,
    pub source: SourceKind,
}

impl Descriptor {
    pub const BYTES: usize = 48;
    pub fn encode(self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        out[..16].copy_from_slice(&self.stream.0);
        out[16..32].copy_from_slice(&self.workload.0);
        out[32..40].copy_from_slice(&self.generation.to_le_bytes());
        out[40] = self.source as u8;
        out
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != Self::BYTES || bytes[41..].iter().any(|&b| b != 0) {
            return Err(invalid());
        }
        let result = Self {
            stream: StreamId(array(bytes, 0)?),
            workload: WorkloadId(array(bytes, 16)?),
            generation: u64::from_le_bytes(array(bytes, 32)?),
            source: match bytes[40] {
                1 => SourceKind::Runtime,
                2 => SourceKind::Build,
                _ => return Err(invalid()),
            },
        };
        if result.stream.0 == [0; 16] || result.workload.0 == [0; 16] || result.generation == 0 {
            return Err(invalid());
        }
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Gap {
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
    pub records: u64,
    pub reason: u32,
}
impl Gap {
    pub const BYTES: usize = 40;
    pub fn encode(self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        for (offset, value) in [
            (0, self.first),
            (8, self.last),
            (16, self.bytes),
            (24, self.records),
        ] {
            out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        out[32..36].copy_from_slice(&self.reason.to_le_bytes());
        out
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != Self::BYTES || bytes[36..] != [0; 4] {
            return Err(invalid());
        }
        let gap = Self {
            first: u64::from_le_bytes(array(bytes, 0)?),
            last: u64::from_le_bytes(array(bytes, 8)?),
            bytes: u64::from_le_bytes(array(bytes, 16)?),
            records: u64::from_le_bytes(array(bytes, 24)?),
            reason: u32::from_le_bytes(array(bytes, 32)?),
        };
        if gap.first == 0 || gap.last < gap.first || gap.reason == 0 || gap.reason > 5 {
            return Err(invalid());
        }
        Ok(gap)
    }
}
