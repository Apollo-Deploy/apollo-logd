use apollo_telemetry_protocol::{MAX_STREAMS, invalid, local::Peer};
use std::{io, path::PathBuf};

pub struct Config {
    pub spool: PathBuf,
    pub socket: PathBuf,
    pub producer: Peer,
    pub agent: Peer,
    pub quota: u64,
    pub segment: u64,
    pub streams: usize,
    pub reserve: u64,
    pub stream_quota: u64,
    pub rate: u64,
}
impl Config {
    pub fn parse(mut args: impl Iterator<Item = String>) -> io::Result<Self> {
        let current = Peer {
            uid: rustix::process::geteuid().as_raw(),
            gid: rustix::process::getegid().as_raw(),
        };
        let mut config = Self {
            spool: "/var/lib/apollo-logd".into(),
            socket: "/run/apollo-logd/capture.sock".into(),
            producer: current,
            agent: current,
            quota: 2 * 1024 * 1024 * 1024,
            segment: 8 * 1024 * 1024,
            streams: MAX_STREAMS,
            reserve: 64 * 1024 * 1024,
            stream_quota: 0,
            rate: 0,
        };
        while let Some(key) = args.next() {
            let value = args.next().ok_or_else(invalid)?;
            match key.as_str() {
                "--spool" => config.spool = value.into(),
                "--socket" => config.socket = value.into(),
                "--producer-uid" => config.producer.uid = value.parse().map_err(|_| invalid())?,
                "--producer-gid" => config.producer.gid = value.parse().map_err(|_| invalid())?,
                "--agent-uid" => config.agent.uid = value.parse().map_err(|_| invalid())?,
                "--agent-gid" => config.agent.gid = value.parse().map_err(|_| invalid())?,
                "--quota-bytes" => config.quota = value.parse().map_err(|_| invalid())?,
                "--segment-bytes" => config.segment = value.parse().map_err(|_| invalid())?,
                "--max-streams" => config.streams = value.parse().map_err(|_| invalid())?,
                "--reserve-bytes" => config.reserve = value.parse().map_err(|_| invalid())?,
                "--stream-quota-bytes" => {
                    config.stream_quota = value.parse().map_err(|_| invalid())?
                }
                "--rate-bytes-per-sec" => config.rate = value.parse().map_err(|_| invalid())?,
                _ => return Err(invalid()),
            }
        }
        if !config.spool.is_absolute()
            || !config.socket.is_absolute()
            || !(1..=MAX_STREAMS).contains(&config.streams)
            || !(128 * 1024..=8 * 1024 * 1024).contains(&config.segment)
            || config.quota > 64 * 1024 * 1024 * 1024
            || config.quota < config.segment * 2 + (config.streams * 512) as u64 + 4096
        {
            return Err(invalid());
        }
        if rustix::process::getrlimit(rustix::process::Resource::Nofile)
            .current
            .is_some_and(|limit| limit < (config.streams * 3 + 64) as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RLIMIT_NOFILE must cover three descriptors per configured stream plus 64",
            ));
        }
        Ok(config)
    }
}
