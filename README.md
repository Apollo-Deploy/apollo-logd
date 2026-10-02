# apollo-logd

A small Linux log capture and write-ahead log daemon for Apollo. MIT licensed.
One epoll thread captures opaque stdout/stderr bytes, sequences bounded records,
spools them to disk and replays them to
[apollo-agent](https://github.com/Apollo-Deploy/apollo-agent).

**Status: partial production qualification.** Production runtime/MicroSandbox
raw capture and builder integration remain unfinished. Closed stream identities
currently retain registry slots: the default 4,096 limit bounds lifetime identities
in a spool, not just active streams. Safe retirement and long churn qualification
remain release blockers. Do not reset state or reuse stream IDs to bypass this limit.

## Architecture

```text
Trusted runtime producer → raw bytes → logd → bounded WAL
                                       ↓
                              authenticated Unix IPC
                                       ↓
                              Agent → remote ingest
                                       ↑
                         durable ACK → WAL reclamation
```

Logd has no cloud, TLS or database client. Agent owns remote authorization and
transport. Agent outages leave capture and local spooling independent, subject
to quota; explicit gap records represent known loss.

## Build and verify

Use Rust 1.88.0 on Linux:

```sh
cargo build --release --locked --bin apollo-logd
cargo fmt --all -- --check
cargo clippy --locked --workspace --lib --bins -- -D warnings
python3 verification/linux.py --binary "$(pwd)/target/release/apollo-logd" --evidence /tmp/logd-qualification.json
```

The qualification harness launches disposable daemon instances, validates
binary replay, quotas, ACK rejection and malformed registration, then measures
resource scaling and throughput. It creates temporary spools and needs available
disk and FD capacity. Wrong-UID process qualification runs only when invoked as
root. Ordinary Rust unit tests are not run. The harness is infrastructure
qualification, not a claim that the production runtime gate is complete.

## Operation

The [systemd unit](deploy/apollo-logd.service) uses an unprivileged `apollo-logd`
account, no capabilities, protected spool/runtime directories and AF_UNIX only.
Create that account before installing the unit. Set producer and Agent UID/GID
values to trusted host identities; the supplied unit uses the existing root Node/
Agent principal. Workloads must not inherit those credentials or socket access.

Flags use separate values: `--spool`, `--socket`, `--producer-uid`, `--producer-gid`,
`--agent-uid`, `--agent-gid`, `--quota-bytes`, `--segment-bytes`, `--max-streams`,
`--reserve-bytes`, `--stream-quota-bytes`, `--rate-bytes-per-sec`.
Defaults: 2 GiB spool, 8 MiB segments, 4,096 identities and 64 MiB free-space reserve.
Set RLIMIT_NOFILE to at least `3 * max-streams + 64` and keep spool capacity fixed.

The trusted producer registers authoritative stream IDs and waits for READY
before starting customer execution. A durable remote ACK, not local receipt,
authorizes reclamation. Runtime relays must handle logd restart and broken pipes;
naive direct customer-to-logd pipes are not a production-safe substitute.

See [local protocol](docs/local-protocol.md), [WAL format](docs/wal-format.md),
[threat model](docs/threat-model.md), [security](SECURITY.md),
[source provenance](docs/source-export.md), and [MIT license](LICENSE).
