#![forbid(unsafe_code)]
#[cfg(target_os = "linux")]
mod capture;
#[cfg(target_os = "linux")]
mod config;
#[cfg(target_os = "linux")]
mod daemon;
#[cfg(target_os = "linux")]
mod wal;

pub fn run() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        daemon::run(config::Config::parse(std::env::args().skip(1))?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "apollo-logd requires Linux",
        ))
    }
}
