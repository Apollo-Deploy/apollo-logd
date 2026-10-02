//! Linux SOCK_SEQPACKET: atomic bounded frames, CLOEXEC rights, exact peers.
use crate::{MAX_PACKET, invalid};
use rustix::net::{self, AddressFamily, SocketAddrUnix, SocketFlags, SocketType};
use std::fs::{self, File, OpenOptions};
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy)]
pub struct Peer {
    pub uid: u32,
    pub gid: u32,
}
impl Peer {
    pub fn authorize(self, fd: impl AsFd) -> io::Result<()> {
        let peer = net::sockopt::socket_peercred(fd)?;
        if peer.uid.as_raw() != self.uid || peer.gid.as_raw() != self.gid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "log peer unauthorized",
            ));
        }
        Ok(())
    }
}

pub fn safe_directory(path: &Path) -> io::Result<()> {
    safe_directory_for(path, rustix::process::geteuid().as_raw())
}

fn safe_directory_for(path: &Path, owner: u32) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(invalid());
    }
    let mut current = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => continue,
            Component::Normal(value) => current.push(value),
            _ => return Err(invalid()),
        }
        let meta = fs::symlink_metadata(&current)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(invalid());
        }
        let sticky_root = meta.uid() == 0 && meta.mode() & 0o1000 != 0;
        if (meta.uid() != 0
            && meta.uid() != rustix::process::geteuid().as_raw()
            && meta.uid() != owner)
            || (meta.mode() & 0o022 != 0 && !sticky_root)
        {
            return Err(invalid());
        }
    }
    let meta = fs::symlink_metadata(path)?;
    if meta.mode() & 0o022 != 0 {
        return Err(invalid());
    }
    Ok(())
}

pub fn private_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return Err(invalid());
    }
    Ok(file)
}

fn socket(nonblock: bool) -> io::Result<OwnedFd> {
    let flags = SocketFlags::CLOEXEC
        | if nonblock {
            SocketFlags::NONBLOCK
        } else {
            SocketFlags::empty()
        };
    let fd = net::socket_with(AddressFamily::UNIX, SocketType::SEQPACKET, flags, None)?;
    net::sockopt::set_socket_send_buffer_size(&fd, MAX_PACKET * 2)?;
    net::sockopt::set_socket_recv_buffer_size(&fd, MAX_PACKET * 2)?;
    Ok(fd)
}

pub struct Listener {
    pub fd: OwnedFd,
    path: PathBuf,
    inode: u64,
    _lock: File,
}
impl Listener {
    pub fn bind(path: &Path) -> io::Result<Self> {
        let parent = path.parent().ok_or_else(invalid)?;
        safe_directory(parent)?;
        let lock = private_file(&path.with_extension("lock"))?;
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
        if let Ok(meta) = fs::symlink_metadata(path) {
            if !meta.file_type().is_socket() || meta.uid() != rustix::process::geteuid().as_raw() {
                return Err(invalid());
            }
            let probe = socket(true)?;
            match net::connect(&probe, &SocketAddrUnix::new(path)?) {
                Err(rustix::io::Errno::CONNREFUSED) => fs::remove_file(path)?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "log socket active",
                    ));
                }
            }
        }
        let fd = socket(true)?;
        net::bind(&fd, &SocketAddrUnix::new(path)?)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
        net::listen(&fd, 32)?;
        let inode = fs::symlink_metadata(path)?.ino();
        Ok(Self {
            fd,
            path: path.to_owned(),
            inode,
            _lock: lock,
        })
    }
    pub fn accept(&self) -> io::Result<OwnedFd> {
        Ok(net::accept_with(
            &self.fd,
            SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
        )?)
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.ino() == self.inode && m.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub fn connect(path: &Path, peer: Peer) -> io::Result<OwnedFd> {
    safe_directory_for(path.parent().ok_or_else(invalid)?, peer.uid)?;
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket() || meta.uid() != peer.uid || meta.mode() & 0o007 != 0 {
        return Err(invalid());
    }
    let fd = socket(true)?;
    net::connect(&fd, &SocketAddrUnix::new(path)?)?;
    peer.authorize(&fd)?;
    Ok(fd)
}

pub fn send(fd: impl AsFd, bytes: &[u8], rights: &[BorrowedFd<'_>]) -> io::Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_PACKET || rights.len() > 2 {
        return Err(invalid());
    }
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
    let mut ancillary = net::SendAncillaryBuffer::new(&mut space);
    if !rights.is_empty() && !ancillary.push(net::SendAncillaryMessage::ScmRights(rights)) {
        return Err(invalid());
    }
    let n = net::sendmsg(
        fd,
        &[IoSlice::new(bytes)],
        &mut ancillary,
        net::SendFlags::NOSIGNAL | net::SendFlags::DONTWAIT,
    )?;
    if n != bytes.len() {
        return Err(invalid());
    }
    Ok(())
}

/// All SCM_RIGHTS descriptors, including excess/truncated input, are RAII-owned.
pub fn receive(fd: impl AsFd, out: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(8))];
    let mut ancillary = net::RecvAncillaryBuffer::new(&mut space);
    let message = net::recvmsg(
        fd,
        &mut [IoSliceMut::new(out)],
        &mut ancillary,
        net::RecvFlags::CMSG_CLOEXEC | net::RecvFlags::DONTWAIT,
    )?;
    let mut rights = Vec::with_capacity(2);
    for cmsg in ancillary.drain() {
        match cmsg {
            net::RecvAncillaryMessage::ScmRights(fds) => {
                for fd in fds {
                    if rights.len() < 2 {
                        rights.push(fd);
                    } else {
                        return Err(invalid());
                    }
                }
            }
            _ => return Err(invalid()),
        }
    }
    if message
        .flags
        .intersects(net::ReturnFlags::TRUNC | net::ReturnFlags::CTRUNC)
    {
        return Err(invalid());
    }
    Ok((message.bytes, rights))
}

pub fn wait(fd: impl AsFd, write: bool, timeout_ms: i32) -> io::Result<()> {
    let flags = if write {
        rustix::event::PollFlags::OUT
    } else {
        rustix::event::PollFlags::IN
    };
    let mut fds = [rustix::event::PollFd::new(&fd, flags)];
    let timeout = rustix::event::Timespec {
        tv_sec: i64::from(timeout_ms / 1000),
        tv_nsec: i64::from(timeout_ms % 1000) * 1_000_000,
    };
    if rustix::event::poll(&mut fds, Some(&timeout))? == 0 {
        return Err(io::ErrorKind::TimedOut.into());
    }
    Ok(())
}
