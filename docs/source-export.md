# Source export

This repository is an independent source snapshot of the named Apollo component.
`SOURCE.json` identifies the upstream repository and base commit, and records
SHA-256 hashes for files copied without changes. The working tree included
uncommitted development, so the base commit alone does not identify this snapshot.
This repository's Git commit is the authoritative public snapshot.

Runtime implementation and protocol files are copied unchanged. Packaging adds
MIT licensing, standalone workspace metadata, documentation and CI. No upstream
Git history, credentials, build outputs or operational evidence is included.

`apollo-telemetry-protocol` is vendored in both Agent and logd repositories so
either can build without a private repository or a pre-published crates.io
package. Keep these copies byte-identical for a given protocol release. Changes
must retain compatibility or explicitly version the protocol before rollout.
