# Contributing

Use Rust 1.88.0. Keep wire versions, signed domains, immutable stream identities
and generation/revision fencing compatible. Explain protocol changes explicitly.
Do not include live credentials, WALs, customer payloads or deployment state.

Follow `AGENTS.md` for permitted verification. CI builds production binaries,
runs formatting and strict Clippy, and executes Linux qualification tools.
Ordinary Rust unit tests are not run under the current implementation-first policy.

Contributions are provided under the MIT license in `LICENSE`.
