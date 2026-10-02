# Contributor instructions

Keep changes scoped and preserve protocol identities and generation fencing.
Use Rust 1.88.0 and keep modules below 500 lines where practical.
Do not add or run ordinary Rust unit tests under the current implementation-first policy.
Use production builds, formatting, scoped Clippy and the supplied qualification tools.
Keep customer bytes opaque and credentials out of diagnostics.
Do not commit credentials, live state, WALs or operational evidence.
