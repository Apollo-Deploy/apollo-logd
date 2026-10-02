//! Trusted orchestration adapter. No ownership is accepted from workload bytes.
use crate::{
    Channel, Descriptor, Gap, MAX_PACKET, MAX_PAYLOAD, invalid, local,
    packet::{self, Kind},
};
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::path::Path;

pub struct Producer {
    fd: OwnedFd,
    staging: Vec<u8>,
    loss: Gap,
}
impl Producer {
    /// Confirms durable registration before the caller starts execution.
    /// `rights` is stdout then stderr. Zero rights selects direct byte feeding.
    pub fn register(
        path: &Path,
        peer: local::Peer,
        descriptor: Descriptor,
        rights: &[OwnedFd],
    ) -> io::Result<Self> {
        if rights.len() > 2 {
            return Err(invalid());
        }
        let fd = local::connect(path, peer)?;
        let mut staging = Vec::with_capacity(MAX_PAYLOAD + 32);
        packet::encode(Kind::Hello, &[1], &mut staging)?;
        local::wait(&fd, true, 2000)?;
        local::send(&fd, &staging, &[])?;
        packet::encode(Kind::Register, &descriptor.encode(), &mut staging)?;
        let rights: Vec<_> = rights.iter().map(AsFd::as_fd).collect();
        local::wait(&fd, true, 2000)?;
        local::send(&fd, &staging, &rights)?;
        local::wait(&fd, false, 2000)?;
        let mut response = [0; 64];
        let (n, fds) = local::receive(&fd, &mut response)?;
        let (kind, body) = packet::decode(&response[..n])?;
        if !fds.is_empty() || kind != Kind::Ready || body != descriptor.encode() {
            return Err(invalid());
        }
        Ok(Self {
            fd,
            staging,
            loss: Gap::default(),
        })
    }

    /// Never waits for logd. Accumulates known transport loss in fixed metadata.
    /// Returns false if any bytes were lost. Caller diagnostics must omit payload.
    pub fn feed(&mut self, channel: Channel, bytes: &[u8]) -> io::Result<bool> {
        if channel == Channel::Control {
            return Err(invalid());
        }
        let mut retained = true;
        for part in bytes.chunks(MAX_PAYLOAD) {
            if self.loss.bytes != 0 {
                packet::encode(Kind::Lost, &self.loss.encode(), &mut self.staging)?;
                match local::send(&self.fd, &self.staging, &[]) {
                    Ok(()) => self.loss = Gap::default(),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.drop_bytes(part.len());
                        retained = false;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            self.staging.clear();
            self.staging.resize(packet::HEADER + 8, 0);
            self.staging[packet::HEADER] = channel as u8;
            self.staging.extend_from_slice(part);
            let body_len = self.staging.len() - packet::HEADER;
            self.staging[..4].copy_from_slice(b"ALP1");
            self.staging[4] = Kind::Data as u8;
            self.staging[6..8].copy_from_slice(&crate::VERSION.to_le_bytes());
            self.staging[8..12].copy_from_slice(&(body_len as u32).to_le_bytes());
            let crc = crc32fast::hash(&self.staging[packet::HEADER..]);
            self.staging[12..16].copy_from_slice(&crc.to_le_bytes());
            match local::send(&self.fd, &self.staging, &[]) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.drop_bytes(part.len());
                    retained = false;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(retained)
    }
    fn drop_bytes(&mut self, n: usize) {
        self.loss.first = 1;
        self.loss.last = 1;
        self.loss.reason = 4;
        self.loss.bytes = self.loss.bytes.saturating_add(n as u64);
        self.loss.records = self.loss.records.saturating_add(1);
    }
    /// Bounded graceful drain; hard process death can lose unsent source bytes.
    pub fn close(mut self) -> io::Result<()> {
        if self.loss.bytes != 0 {
            packet::encode(Kind::Lost, &self.loss.encode(), &mut self.staging)?;
            local::wait(&self.fd, true, 2000)?;
            local::send(&self.fd, &self.staging, &[])?;
        }
        packet::encode(Kind::Close, &[], &mut self.staging)?;
        local::wait(&self.fd, true, 2000)?;
        local::send(&self.fd, &self.staging, &[])
    }
    pub fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

const _: () = assert!(MAX_PAYLOAD + 32 <= MAX_PACKET);
