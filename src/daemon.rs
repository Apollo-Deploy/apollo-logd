//! One bounded synchronous Linux readiness loop; no worker threads or tasks.
mod connection;
mod delivery;
mod input;
use crate::{
    capture::registry::Registry,
    config::Config,
    wal::{Cursor, Wal},
};
use apollo_telemetry_protocol::{MAX_PACKET, MAX_PAYLOAD, local, record};
use connection::{Connection, Role};
use rustix::event::epoll::{self, EventData, EventFlags};
use std::{
    io,
    os::fd::OwnedFd,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
const LISTENER: u64 = 1;
const CONNECTION: u64 = 16;
const SOURCE: u64 = 65536;
const TICK: Duration = Duration::from_millis(50);

pub(super) struct Engine {
    config: Config,
    registry: Registry,
    wal: Wal,
    epoll: OwnedFd,
    listener: local::Listener,
    connections: Vec<Option<Connection>>,
    sources: Vec<Option<OwnedFd>>,
    agent: Option<usize>,
    input: Vec<u8>,
    scratch: Vec<u8>,
    encoded: Vec<u8>,
    packet: Vec<u8>,
    batch: Vec<u8>,
    cursor: Cursor,
    registration: usize,
    pending: bool,
    last_commit: Instant,
    counters: [u64; 9],
    sent_gaps: Vec<(crate::capture::registry::StateRef, u64, u64)>,
    last_counters: Instant,
    shutdown: Arc<AtomicBool>,
}
pub fn run(config: Config) -> io::Result<()> {
    let mut registry = Registry::new(config.streams);
    let mut wal = Wal::open(&config, &mut registry)?;
    // Open sources cannot survive process death. Record unknown source downtime;
    // orchestration may reconcile the same registration after restart.
    for state in &mut registry.states {
        if !state.closing && !state.closed {
            state.loss(0, 1, 5)?;
        }
    }
    wal.sync(&mut registry)?;
    let listener = local::Listener::bind(&config.socket)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(signal, Arc::clone(&shutdown))?;
    }
    let poll = epoll::create(epoll::CreateFlags::CLOEXEC)?;
    epoll::add(
        &poll,
        &listener.fd,
        EventData::new_u64(LISTENER),
        EventFlags::IN,
    )?;
    let mut engine = Engine {
        connections: (0..config.streams + 33).map(|_| None).collect(),
        sources: (0..config.streams * 2).map(|_| None).collect(),
        config,
        registry,
        wal,
        epoll: poll,
        listener,
        agent: None,
        input: vec![0; MAX_PACKET],
        scratch: vec![0; record::HEADER + MAX_PAYLOAD],
        encoded: Vec::with_capacity(record::HEADER + MAX_PAYLOAD),
        packet: Vec::with_capacity(MAX_PACKET),
        batch: Vec::with_capacity(MAX_PACKET),
        cursor: Cursor::default(),
        registration: 0,
        pending: false,
        last_commit: Instant::now(),
        counters: [0; 9],
        sent_gaps: Vec::with_capacity(4096),
        last_counters: Instant::now(),
        shutdown,
    };
    engine.event_loop()
}
impl Engine {
    fn event_loop(&mut self) -> io::Result<()> {
        let mut events: Vec<epoll::Event> = Vec::with_capacity(256);
        let timeout = rustix::event::Timespec {
            tv_sec: 0,
            tv_nsec: 50_000_000,
        };
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                let deadline = Instant::now() + TICK;
                for index in 0..self.sources.len() {
                    if Instant::now() >= deadline {
                        break;
                    }
                    self.capture(index)?;
                }
                self.persist_gaps()?;
                self.finish_closed()?;
                self.wal.sync(&mut self.registry)?;
                return Ok(());
            }
            events.clear();
            match epoll::wait(
                &self.epoll,
                rustix::buffer::spare_capacity(&mut events),
                Some(&timeout),
            ) {
                Ok(_) => {}
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            }
            for event in &events {
                let token = event.data.u64();
                if token == LISTENER {
                    self.accept()?;
                } else if token >= SOURCE {
                    self.capture((token - SOURCE) as usize)?;
                } else if token >= CONNECTION {
                    let index = (token - CONNECTION) as usize;
                    if self.connections.get(index).is_some_and(Option::is_some)
                        && let Err(error) = self.receive(index)
                    {
                        if matches!(
                            error.kind(),
                            io::ErrorKind::InvalidData
                                | io::ErrorKind::PermissionDenied
                                | io::ErrorKind::UnexpectedEof
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::ConnectionReset
                        ) {
                            self.disconnect(index)?;
                        } else if error.kind() != io::ErrorKind::WouldBlock
                            && error.kind() != io::ErrorKind::Interrupted
                        {
                            return Err(error);
                        }
                    }
                }
            }
            if self.last_commit.elapsed() >= TICK {
                self.persist_gaps()?;
                self.finish_closed()?;
                self.wal.sync(&mut self.registry)?;
                self.wal.reclaim(&mut self.registry, &mut self.scratch)?;
                self.expire()?;
                self.last_commit = Instant::now();
            }
            self.deliver()?;
        }
    }
    fn disconnect(&mut self, index: usize) -> io::Result<()> {
        if let Some(connection) = self.connections[index].take() {
            let _ = epoll::delete(&self.epoll, &connection.fd);
            if let Role::Feed(stream) = connection.role {
                let slot = self.registry.resolve(stream)?;
                if !self.registry.states[slot].closing {
                    self.registry.states[slot].loss(0, 1, 5)?;
                }
            }
        }
        if self.agent == Some(index) {
            self.agent = None;
            self.pending = false;
            self.batch.clear();
            self.cursor = Cursor::default();
        }
        Ok(())
    }
    fn expire(&mut self) -> io::Result<()> {
        for index in 0..self.connections.len() {
            if self.connections[index].as_ref().is_some_and(|c| {
                matches!(c.role, Role::Hello | Role::Register)
                    && c.created.elapsed() > Duration::from_secs(2)
            }) {
                self.disconnect(index)?;
            }
        }
        Ok(())
    }
}
