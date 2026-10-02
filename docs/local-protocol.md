# Local protocol v1

Transport is Linux Unix `SOCK_SEQPACKET`, nonblocking, CLOEXEC, maximum packet
131,072 bytes. Logd owns the listener; Agent reconnects to it. No cloud address,
TLS material, tenant token or database connection enters logd.

The 16-byte packet prefix is `ALP1`, kind u8, zero u8, version u16=1, body length
u32, body CRC32 u32. Length/CRC/type/reserved bytes are checked before dispatch.
Packet truncation and ancillary truncation reject the peer and close received
FDs through RAII. At most two SCM_RIGHTS FDs are accepted, only on registration.

| Kind | Body / effect |
|---|---|
| 1 HELLO | One byte: producer=1 or Agent=2; role requires exact configured UID/GID |
| 2 REGISTER | 48-byte descriptor; producer can attach stdout then stderr FIFO read FDs |
| 3 READY | Exact descriptor; registration is checkpointed and FDs attached before reply |
| 4 BATCH | Concatenated ALR1 records, bounded by packet maximum |
| 5 ACK | stream16 + generation u64 + cumulative sequence u64 |
| 6 CLOSE | Empty producer body; drain, persist close, retain until ACK |
| 7 DATA | channel u8 + seven zeros + 1..65,536 opaque bytes |
| 8 LOST | 40-byte producer gap, reason=4; logd assigns its sequence range |
| 9 COUNTERS | Nine u64 values; at most once per second when delivery is idle |

Descriptor: stream16, workload16, generation u64, source kind u8 (runtime=1,
build=2), seven zeros. Zero IDs/generation and conflicting reuse are invalid.
With zero attached FDs, the registered socket feeds bytes directly. With one
or two FDs, the control socket may close after READY; logd retains the received
read ends and observes EOF. Register before starting execution. There is no
workload-controlled ownership field in DATA.

The source-side `Producer` library offers registration, bounded nonblocking
`feed`, and a two-second graceful close budget. Transport saturation coalesces
known unsent bytes into LOST. An ungraceful producer death can lose its unsent
loss counter; open-stream recovery reports unknown downtime. Pipe adapters
must isolate logging failures from the application: direct application writes
to a pipe whose only reader dies can receive SIGPIPE. The MicroSandbox relay
adapter must use nonblocking producer feeding and handle reconnect/loss; it is
not yet wired into the provisioned runtime artifact.

Agent receives descriptors, validates signed ownership, durably accepts an
entire batch, then returns cumulative ACKs. The shared Receiver never ACKs a
failed durable acceptance. Disconnect at any point causes stable-identity
replay. Duplicate ACKs are harmless; wrong generation, ACK beyond the sent or
durable frontier, and ACK inside a delivered gap reject the connection.

Counters in order: active streams, captured bytes, WAL bytes, unacknowledged
logical positions, dropped bytes, gap events, IPC connected, replayed/sent
records, recovery errors. They are process-lifetime counters, not billing
statistics. Fatal recovery errors prevent connection, so the final recovery
error must be observed through infrastructure service diagnostics.
