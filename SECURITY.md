# Security

This is an early public source release, not a completed production qualification.
Do not expose the local IPC socket, credentials, enrollment bundle or spool to
customer workloads. A trusted host producer and Agent must use the configured
UID/GID identities; remote ingest must independently validate signed node,
workload, generation and stream assignments.

Please report vulnerabilities using this repository's GitHub **Security →
Report a vulnerability** feature. Do not post credentials, customer payloads or
exploit details affecting deployed systems in public issues.

Host root can impersonate local processes. On BYOC machines, server-side
assignment checks provide cross-tenant write isolation; host-reported resource
measurements are not attested.
