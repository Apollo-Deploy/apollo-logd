//! Agent-side adapter. Only the Agent's durable sink may authorize an ACK.
use crate::{
    Descriptor, MAX_PACKET, StreamId, invalid, local,
    packet::{self, Kind},
    record,
};
use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;

/// Implementations must authenticate stream ownership outside logd, deduplicate
/// stable (stream,generation,sequence), and durably accept before returning.
pub trait DurableSink {
    fn register(&mut self, descriptor: Descriptor) -> io::Result<()>;
    fn accept(&mut self, encoded_records: &[u8]) -> io::Result<()>;
    /// Fixed node-wide counters in the order documented by local protocol v1.
    fn counters(&mut self, _values: [u64; 9]) {}
}

pub struct Receiver {
    fd: OwnedFd,
    input: Vec<u8>,
    output: Vec<u8>,
}
impl Receiver {
    pub fn connect(path: &Path, logd: local::Peer) -> io::Result<Self> {
        let fd = local::connect(path, logd)?;
        let mut output = Vec::with_capacity(64);
        packet::encode(Kind::Hello, &[2], &mut output)?;
        local::wait(&fd, true, 2000)?;
        local::send(&fd, &output, &[])?;
        Ok(Self {
            fd,
            input: vec![0; MAX_PACKET],
            output,
        })
    }
    pub fn receive(&mut self, sink: &mut impl DurableSink) -> io::Result<()> {
        local::wait(&self.fd, false, 2000)?;
        let (n, rights) = local::receive(&self.fd, &mut self.input)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if !rights.is_empty() {
            return Err(invalid());
        }
        let (kind, body) = packet::decode(&self.input[..n])?;
        match kind {
            Kind::Register => sink.register(Descriptor::decode(body)?)?,
            Kind::Batch => {
                let mut rest = body;
                while !rest.is_empty() {
                    let n = record::length(rest)?;
                    let record = rest.get(..n).ok_or_else(invalid)?;
                    record::decode(record)?;
                    rest = &rest[n..];
                }
                sink.accept(body)?;
                let mut rest = body;
                while !rest.is_empty() {
                    let n = record::length(rest)?;
                    let (header, _) = record::decode(&rest[..n])?;
                    ack(
                        &self.fd,
                        &mut self.output,
                        header.stream,
                        header.generation,
                        header.sequence,
                    )?;
                    rest = &rest[n..];
                }
            }
            Kind::Counters if body.len() == 72 => {
                let mut values = [0; 9];
                for (index, value) in values.iter_mut().enumerate() {
                    *value = u64::from_le_bytes(crate::array(body, index * 8)?);
                }
                sink.counters(values);
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
}
fn ack(
    fd: &OwnedFd,
    out: &mut Vec<u8>,
    stream: StreamId,
    generation: u64,
    sequence: u64,
) -> io::Result<()> {
    let mut body = [0; 32];
    body[..16].copy_from_slice(&stream.0);
    body[16..24].copy_from_slice(&generation.to_le_bytes());
    body[24..32].copy_from_slice(&sequence.to_le_bytes());
    packet::encode(Kind::Ack, &body, out)?;
    local::wait(fd, true, 2000)?;
    local::send(fd, out, &[])
}
