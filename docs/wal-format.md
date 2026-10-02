# WAL format v1

All integers are little endian. CRC is IEEE CRC32 (`crc32fast`). Checksums detect
corruption; private filesystem permissions supply integrity against local users.

Segments are at most 8 MiB by default. Their 32-byte header is:

| Offset | Bytes | Value |
|---|---:|---|
| 0 | 8 | `ALWAL01\0` |
| 8 | 2 | Version 1 |
| 10 | 6 | Zero |
| 16 | 8 | Nonzero monotonic segment ID |
| 24 | 4 | Zero |
| 28 | 4 | CRC of bytes 0..28 |

The filename is `segment-{id:020}.wal`; recovery validates the embedded ID and
contiguous retained segment IDs. `segment-new` holds only an unpublished header.
Creation writes/syncs that header, renames atomically, then syncs the directory.
Recovery can discard an unpublished header without discarding customer records.

## Records

Each record has a 72-byte header, then at most 65,536 opaque payload bytes:

| Offset | Bytes | Value |
|---|---:|---|
| 0 | 4 | `ALR1` |
| 4 | 4 | Total length, validated before any payload read |
| 8 | 4 | CRC of bytes 12..end |
| 12 | 1 | Bytes=1, Gap=2, Close=3 |
| 13 | 1 | Control=0, stdout=1, stderr=2 |
| 14 | 2 | Flags, continuation bit=1; other bits invalid |
| 16 | 16 | Stream ID |
| 32 | 8 | Generation |
| 40 | 8 | Sequence (gap uses last missing sequence) |
| 48 | 8 | Realtime Unix ns |
| 56 | 8 | Monotonic ns (boot-local) |
| 64 | 4 | Payload length |
| 68 | 4 | Zero |

Gap payload is 40 bytes: first missing sequence, last missing sequence, dropped
bytes, dropped record count (four u64), reason u32, zero u32. Reasons are quota
1, rate 2, stale-generation source 3, producer transport loss 4, unknown source
downtime 5. Unknown downtime uses zero known bytes and one logical gap position;
it does not claim the unavailable byte/record count is known. A close has no
payload. Sequences begin at 1 and cannot wrap.

## Reserved checkpoint/loss journal

`state` has a 64-byte `ALSTATE1` header (capacity u64 at offset 8, zeros 16..60,
CRC at 60), then two 256-byte slots per configured identity. It is physically
preallocated before accepting sources: 2,097,216 bytes for 4,096 identities.
Each slot has `ALS1`, version u64 at 8, descriptor at 16, sequence at 64,
ACK at 72, closing/closed bits at 80, close sequence at 88, frozen gap at 96,
pending gap at 136, frozen realtime/monotonic timestamps at 176/184, reserved
zeros through 252, and CRC at 252. Alternating slots preserve the previous
complete checkpoint during a torn update. Both invalid slots fail closed.

WAL fdatasync precedes checkpoint fdatasync. Segment deletion follows the
persisted ACK checkpoint and is followed by directory sync. No unacknowledged
record is removed to make room. A fully acknowledged active segment is closed
and unlinked before its replacement header is allocated, allowing reclamation
even at ENOSPC. A crash between deletion and replacement is recoverable from
the durable ACK checkpoint. Normal operation leaves one empty segment.
Sequence range information is
stored in records and the per-stream checkpoint, not an unbounded RAM index.

Recovery scans fixed-size scratch, verifies CRC/type/identity/generation and
contiguous per-stream ranges, reconciles promoted loss markers, and rebuilds
sequence and close state. Only a short final prefix or a length-validated
incomplete final record is truncated. Complete CRC damage, nonfinal truncation,
missing checkpointed data, and internal holes stop startup while preserving
files for diagnosis. It never scans past corruption pretending older data was
successfully replayed.

The normal group-commit interval is 50 ms. Kernel/device stalls can extend it;
unsynced records and loss metadata can disappear on power failure. Once logd
sends a record, both its WAL and checkpoint have completed fdatasync. Remote
ACK means the Agent's authenticated sink accepted it durably. Storage must
honor fdatasync and directory sync. Preallocation cannot make EIO, filesystem
corruption, or storage-wide failure safe; these stop the daemon.
