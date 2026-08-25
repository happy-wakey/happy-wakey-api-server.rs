# happy-wakey-api-server.rs

This repository is the contract-first JSON API and persistence boundary for
Happy Wakey. It serves authenticated alarm, occurrence-transition, and sync
endpoints using Axum and SeaORM.

## Security and authority boundaries

- Shared Auth is the sole identity authority. The API accepts only bounded
  bearer credentials and introspects them over HTTPS with redirects disabled.
- Every customer-owned query is scoped to the verified Shared Auth subject.
- The transition reducer is the sole occurrence-state authority. Stale or
  invalid transitions do not mutate the occurrence.
- Idempotency receipts and occurrence updates share one database transaction.
- The service never performs schema migration or DDL at startup.
- Secrets come from the runtime environment and must never be committed.

## Runtime configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | yes | PostgreSQL or CockroachDB connection string |
| `HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET` | yes | Service credential for Shared Auth introspection |
| `HAPPY_WAKEY_API_BIND` | no | Listener address; defaults to `0.0.0.0:8080` |
| `HAPPY_WAKEY_SHARED_AUTH_BASE` | no | HTTPS Shared Auth base URL |
| `HAPPY_WAKEY_SHARED_AUTH_AUDIENCE` | no | Required token audience; defaults to `happy-wakey` |

## Dependency and validation workflow

Use the released `zed-pkg` CLI as the repository dependency authority:

```sh
zed validate
zed install --adapter rust
zed run cargo test --locked
```

The full source gate also runs formatting and warning-free Clippy checks:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

Commit `.zpkg.lock` only when every entry names a portable immutable registry
source. A lock produced against a workstation-local `file://` registry is
validation evidence, not a distributable project lock.

The database schema and deployment manifests are release-owned external
contracts. This repository contains application entities and behavior only.
