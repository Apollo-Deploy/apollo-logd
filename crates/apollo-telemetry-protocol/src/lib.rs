#![forbid(unsafe_code)]
//! Bounded, provider-neutral local log transport. Payloads are opaque bytes.
#[cfg(target_os = "linux")]
pub mod local;
pub mod packet;
#[cfg(target_os = "linux")]
pub mod producer;
#[cfg(target_os = "linux")]
pub mod receiver;
pub mod record;
pub mod types;

pub const VERSION: u16 = 1;
pub const MAX_PAYLOAD: usize = 64 * 1024;
pub const MAX_PACKET: usize = 128 * 1024;
pub const MAX_STREAMS: usize = 4096;
pub use types::*;

pub fn invalid() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid log protocol")
}

pub(crate) fn array<const N: usize>(bytes: &[u8], offset: usize) -> std::io::Result<[u8; N]> {
    bytes
        .get(offset..offset + N)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())
}
