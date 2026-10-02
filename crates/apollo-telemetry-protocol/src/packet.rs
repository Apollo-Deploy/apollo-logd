use crate::{MAX_PACKET, VERSION, array, invalid};
use std::io;
pub const HEADER: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Kind {
    Hello = 1,
    Register = 2,
    Ready = 3,
    Batch = 4,
    Ack = 5,
    Close = 6,
    Data = 7,
    Lost = 8,
    Counters = 9,
}

pub fn encode(kind: Kind, body: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
    if body.len() > MAX_PACKET - HEADER {
        return Err(invalid());
    }
    out.clear();
    out.extend_from_slice(b"ALP1");
    out.push(kind as u8);
    out.push(0);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
    out.extend_from_slice(body);
    Ok(())
}

pub fn decode(bytes: &[u8]) -> io::Result<(Kind, &[u8])> {
    if bytes.len() < HEADER
        || bytes.len() > MAX_PACKET
        || bytes[..4] != *b"ALP1"
        || bytes[5] != 0
        || u16::from_le_bytes(array(bytes, 6)?) != VERSION
        || u32::from_le_bytes(array(bytes, 8)?) as usize != bytes.len() - HEADER
        || u32::from_le_bytes(array(bytes, 12)?) != crc32fast::hash(&bytes[HEADER..])
    {
        return Err(invalid());
    }
    let kind = match bytes[4] {
        1 => Kind::Hello,
        2 => Kind::Register,
        3 => Kind::Ready,
        4 => Kind::Batch,
        5 => Kind::Ack,
        6 => Kind::Close,
        7 => Kind::Data,
        8 => Kind::Lost,
        9 => Kind::Counters,
        _ => return Err(invalid()),
    };
    Ok((kind, &bytes[HEADER..]))
}
