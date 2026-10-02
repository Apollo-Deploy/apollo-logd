# Threat model

Workload bytes, lengths, descriptors from unauthorized peers, stale messages,
and all bytes recovered from disk are untrusted. Payload is never decoded as
text/JSON, never indexed, and never copied into diagnostic messages.

`SO_PEERCRED` checks exact effective UID/GID at role selection. The listener and
spool require absolute, symlink-free protected directory paths. Root-owned
sticky ancestors such as `/tmp` are allowed only above the private final
parent. Files require NOFOLLOW, owner=current daemon UID, regular type, mode
0600 and one link. A spool flock excludes a second writer. Socket cleanup
requires the owning UID, socket type, and refused connection; normal cleanup
checks the captured inode. The socket has mode 0660. Received FDs must be FIFO
read ends, become nonblocking/CLOEXEC, and use OwnedFd ownership.

Producer and Agent principals are trusted infrastructure. Configuring both as
the same UID gives them the same authority: such a producer can claim Agent
role and ACK. Use distinct service identities where these are separate trust
boundaries. Host root is inherently trusted and can impersonate either. Guest
workloads must never share these host credentials or mount the socket/spool.
The service unit uses root as the trusted existing Node/Agent principal; it
does not grant guests access.

No untrusted length controls allocation. Input, encoding and batch buffers are
fixed-bounded; stream, FD, unfinished connection and sent-gap tables have hard
caps. Replay scans disk and does not load backlog. The spool quota includes the
preallocated loss journal. Malformed packets disconnect only their peer;
corrupt durable storage fails startup without erasing evidence.

Quota, reserved-headroom and rate loss generate distinct control records.
Checkpoint preallocation reserves room for loss metadata when the data quota
is full. This cannot promise durable markers during device failure or a
filesystem that cannot overwrite reserved extents. Customer safety during
logd death or a blocked device depends on the nonblocking runtime relay; a
naive direct workload-to-logd pipe is not a production-safe replacement.

Verification is process-boundary Linux qualification. The current repository
instruction forbids adding/running Rust unit tests; those unit/property suites
were not added or run. Coverage-guided Rust fuzz campaigns also remain
unqualified. These are separate release evidence gaps alongside actual
MicroSandbox/build execution wiring.
