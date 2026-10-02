use super::*;
use crate::capture::registry::StateRef;
use apollo_telemetry_protocol::{
    Descriptor, StreamId, invalid,
    packet::{self, Kind},
};
use std::os::fd::AsFd;

pub(super) enum Role {
    Hello,
    Register,
    Feed(StateRef),
    Attached,
    Agent,
}
pub(super) struct Connection {
    pub fd: OwnedFd,
    pub role: Role,
    pub created: Instant,
}
impl Engine {
    pub(super) fn accept(&mut self) -> io::Result<()> {
        for _ in 0..32 {
            let fd = match self.listener.accept() {
                Ok(fd) => fd,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            };
            if self
                .connections
                .iter()
                .flatten()
                .filter(|c| matches!(c.role, Role::Hello | Role::Register))
                .count()
                >= 32
            {
                continue;
            }
            if self.config.producer.authorize(&fd).is_err()
                && self.config.agent.authorize(&fd).is_err()
            {
                continue;
            }
            let Some(index) = self.connections.iter().position(Option::is_none) else {
                continue;
            };
            epoll::add(
                &self.epoll,
                &fd,
                EventData::new_u64(CONNECTION + index as u64),
                EventFlags::IN,
            )?;
            self.connections[index] = Some(Connection {
                fd,
                role: Role::Hello,
                created: Instant::now(),
            });
        }
        Ok(())
    }
    pub(super) fn receive(&mut self, index: usize) -> io::Result<()> {
        for _ in 0..4 {
            let Some(connection) = &self.connections[index] else {
                return Ok(());
            };
            let (length, rights) = local::receive(&connection.fd, &mut self.input)?;
            if length == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let (kind, body) = packet::decode(&self.input[..length])?;
            if !rights.is_empty() && !matches!(connection.role, Role::Register) {
                return Err(invalid());
            }
            match connection.role {
                Role::Hello => {
                    if kind != Kind::Hello || body.len() != 1 {
                        return Err(invalid());
                    }
                    let role = match body[0] {
                        1 => {
                            self.config.producer.authorize(&connection.fd)?;
                            Role::Register
                        }
                        2 if self.agent.is_none() => {
                            self.config.agent.authorize(&connection.fd)?;
                            self.agent = Some(index);
                            self.registration = 0;
                            self.cursor = Cursor::default();
                            self.pending = false;
                            self.sent_gaps.clear();
                            for state in &mut self.registry.states {
                                state.sent = state.acknowledged;
                            }
                            Role::Agent
                        }
                        _ => return Err(invalid()),
                    };
                    self.connections[index].as_mut().ok_or_else(invalid)?.role = role;
                }
                Role::Register => {
                    if kind != Kind::Register {
                        return Err(invalid());
                    }
                    let descriptor = Descriptor::decode(body)?;
                    self.register(index, descriptor, rights)?;
                }
                Role::Agent => {
                    if kind != Kind::Ack || body.len() != 32 {
                        return Err(invalid());
                    }
                    let mut id = [0; 16];
                    id.copy_from_slice(&body[..16]);
                    let mut generation = [0; 8];
                    generation.copy_from_slice(&body[16..24]);
                    let mut through = [0; 8];
                    through.copy_from_slice(&body[24..]);
                    let through = u64::from_le_bytes(through);
                    let stream = self.registry.find(StreamId(id))?;
                    if self.sent_gaps.iter().any(|&(index, first, last)| {
                        index == stream && through >= first && through < last
                    }) {
                        return Err(invalid());
                    }
                    self.registry
                        .ack(StreamId(id), u64::from_le_bytes(generation), through)?;
                    self.sent_gaps
                        .retain(|&(index, _, last)| index != stream || last > through);
                }
                Role::Feed(stream) => self.feed(stream, kind, length)?,
                Role::Attached => return Err(invalid()),
            }
        }
        Ok(())
    }
    fn register(
        &mut self,
        index: usize,
        descriptor: Descriptor,
        rights: Vec<OwnedFd>,
    ) -> io::Result<()> {
        if rights.len() > 2 {
            return Err(invalid());
        }
        for fd in &rights {
            let flags = rustix::fs::fcntl_getfl(fd)?;
            let stat = rustix::fs::fstat(fd)?;
            if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Fifo
                || flags.contains(rustix::fs::OFlags::WRONLY)
            {
                return Err(invalid());
            }
            rustix::fs::fcntl_setfl(fd, flags | rustix::fs::OFlags::NONBLOCK)?;
        }
        let stream = self.registry.register(descriptor)?;
        let slot = stream.slot();
        if self.sources[slot * 2..slot * 2 + 2]
            .iter()
            .any(Option::is_some)
            || self
                .connections
                .iter()
                .flatten()
                .any(|c| matches!(c.role,Role::Feed(s) if s==stream))
        {
            return Err(invalid());
        }
        self.wal.sync(&mut self.registry)?;
        let attached = !rights.is_empty();
        for (channel, fd) in rights.into_iter().enumerate() {
            let source = slot * 2 + channel;
            if let Err(error) = epoll::add(
                &self.epoll,
                &fd,
                EventData::new_u64(SOURCE + source as u64),
                EventFlags::IN,
            ) {
                for old in &mut self.sources[slot * 2..slot * 2 + 2] {
                    if let Some(fd) = old.take() {
                        let _ = epoll::delete(&self.epoll, fd.as_fd());
                    }
                }
                return Err(error.into());
            }
            self.sources[source] = Some(fd);
        }
        let connection = self.connections[index].as_mut().ok_or_else(invalid)?;
        connection.role = if attached {
            Role::Attached
        } else {
            Role::Feed(stream)
        };
        packet::encode(Kind::Ready, &descriptor.encode(), &mut self.encoded)?;
        if let Err(error) = local::send(&connection.fd, &self.encoded, &[]) {
            for old in &mut self.sources[slot * 2..slot * 2 + 2] {
                if let Some(fd) = old.take() {
                    let _ = epoll::delete(&self.epoll, &fd);
                }
            }
            self.disconnect(index)?;
            if error.kind() != io::ErrorKind::WouldBlock {
                return Err(error);
            }
        }
        Ok(())
    }
}
