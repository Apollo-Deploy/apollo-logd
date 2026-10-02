use crate::{Channel, Gap, MAX_PAYLOAD, StreamId, array, invalid};
use std::io;
pub const HEADER: usize = 72;
pub const CONTINUATION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordKind {
    Bytes = 1,
    Gap = 2,
    Close = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub kind: RecordKind,
    pub channel: Channel,
    pub flags: u16,
    pub stream: StreamId,
    pub generation: u64,
    pub sequence: u64,
    pub realtime_ns: u64,
    pub monotonic_ns: u64,
}

pub fn encode(header: Header, payload: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
    if payload.len() > MAX_PAYLOAD {
        return Err(invalid());
    }
    out.clear();
    out.resize(HEADER, 0);
    out[..4].copy_from_slice(b"ALR1");
    out[4..8].copy_from_slice(&((HEADER + payload.len()) as u32).to_le_bytes());
    out[12] = header.kind as u8;
    out[13] = header.channel as u8;
    out[14..16].copy_from_slice(&header.flags.to_le_bytes());
    out[16..32].copy_from_slice(&header.stream.0);
    for (offset, value) in [
        (32, header.generation),
        (40, header.sequence),
        (48, header.realtime_ns),
        (56, header.monotonic_ns),
    ] {
        out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
    out[64..68].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32fast::hash(&out[12..]);
    out[8..12].copy_from_slice(&crc.to_le_bytes());
    decode(out)?;
    Ok(())
}

pub fn length(prefix: &[u8]) -> io::Result<usize> {
    if prefix.len() < 8 || prefix[..4] != *b"ALR1" {
        return Err(invalid());
    }
    let length = u32::from_le_bytes(array(prefix, 4)?) as usize;
    if !(HEADER..=HEADER + MAX_PAYLOAD).contains(&length) {
        return Err(invalid());
    }
    Ok(length)
}

pub fn decode(bytes: &[u8]) -> io::Result<(Header, &[u8])> {
    if bytes.len() < HEADER
        || length(bytes)? != bytes.len()
        || bytes[68..72] != [0; 4]
        || u32::from_le_bytes(array(bytes, 8)?) != crc32fast::hash(&bytes[12..])
        || u32::from_le_bytes(array(bytes, 64)?) as usize != bytes.len() - HEADER
    {
        return Err(invalid());
    }
    let header = Header {
        kind: match bytes[12] {
            1 => RecordKind::Bytes,
            2 => RecordKind::Gap,
            3 => RecordKind::Close,
            _ => return Err(invalid()),
        },
        channel: Channel::try_from(bytes[13])?,
        flags: u16::from_le_bytes(array(bytes, 14)?),
        stream: StreamId(array(bytes, 16)?),
        generation: u64::from_le_bytes(array(bytes, 32)?),
        sequence: u64::from_le_bytes(array(bytes, 40)?),
        realtime_ns: u64::from_le_bytes(array(bytes, 48)?),
        monotonic_ns: u64::from_le_bytes(array(bytes, 56)?),
    };
    if header.stream.0 == [0; 16]
        || header.generation == 0
        || header.sequence == 0
        || header.flags & !CONTINUATION != 0
    {
        return Err(invalid());
    }
    let payload = &bytes[HEADER..];
    match header.kind {
        RecordKind::Bytes if header.channel == Channel::Control || payload.is_empty() => {
            return Err(invalid());
        }
        RecordKind::Gap => {
            let gap = Gap::decode(payload)?;
            if gap.last != header.sequence
                || header.channel != Channel::Control
                || header.flags != 0
            {
                return Err(invalid());
            }
        }
        RecordKind::Close
            if !payload.is_empty() || header.channel != Channel::Control || header.flags != 0 =>
        {
            return Err(invalid());
        }
        _ => {}
    }
    Ok((header, payload))
}
