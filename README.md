# happy-wakey-api-server.rs

This repository is the contract-first JSON API and persistence boundary for
Happy Wakey. It serves authenticated alarm, occurrence-transition, and sync
endpoints using Axum and SeaORM.

## Security and authority boundaries

- Shared Auth is the sole identity authority. The API accepts only bounded
  bearer credentials and introspects them through the canonical versioned
  Shared Auth HTTPS contract with redirects disabled and bounded responses.
  The service credential is independent of the end-user bearer and is never
  persisted or logged.
- Every customer-owned query is scoped to the verified Shared Auth subject.
- The transition reducer is the sole occurrence-state authority. Stale or
  invalid transitions do not mutate the occurrence.
- Idempotency receipts and occurrence updates share one database transaction.
- The service never performs schema migration or DDL at startup.
- Secrets come from the runtime environment and must never be committed.

## Web-to-API interaction modes

The surrounding Happy Wakey system supports four deliberately distinct paths:

1. Direct database reads run only through `happy-wakey-lib-core::ReadContext`.
   That capability exposes subject-scoped reads and no write or raw-connection
   escape hatch.
2. Stateless HTTPS uses the normal Axum endpoints. Deployments must terminate
   TLS before this service and forward the authorization header unchanged.
3. Stateful TCP is optional and always uses TLS. It uses bounded four-byte
   length-delimited frames, bounded connections, an idle timeout, and a request
   limit. Every frame carries a bearer and is re-introspected; a connection
   never caches identity.
4. Async work uses JetStream, not Core NATS request/reply. The client first
   registers an idempotent operation over authenticated HTTPS. The API stores
   only the verified subject and operation in its database outbox. A
   credential-free signal then enters a pre-provisioned file-backed work-queue
   stream. The API commits the response, publishes it with a deterministic
   message ID, waits for the JetStream publish acknowledgement, and only then
   acknowledges the request. Redelivery therefore replays the stored response
   without repeating the database read.

The application validates the existing streams and durable consumer at
startup; it does not create or mutate broker topology. Invalid signals and
unknown operation IDs are terminated, while transient database failures are
negatively acknowledged for bounded redelivery. Bearers and service
credentials are excluded from the outbox, JetStream payloads, dead-letter
paths, and ores-otel event fields.

## Runtime configuration

| Variable | Required | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | yes | PostgreSQL or CockroachDB connection string |
| `HAPPY_WAKEY_SHARED_AUTH_INTROSPECT_SECRET` | yes | Service credential for Shared Auth introspection |
| `HAPPY_WAKEY_API_BIND` | no | Listener address; defaults to `0.0.0.0:8080` |
| `HAPPY_WAKEY_SHARED_AUTH_BASE` | no | HTTPS Shared Auth base URL |
| `HAPPY_WAKEY_SHARED_AUTH_AUDIENCE` | no | Required token audience; defaults to `happy-wakey` |
| `HAPPY_WAKEY_API_TCP_BIND` | no | Enables the persistent TLS listener |
| `HAPPY_WAKEY_API_TCP_TLS_CERT` | with TCP | PEM certificate chain |
| `HAPPY_WAKEY_API_TCP_TLS_KEY` | with TCP | PEM private key |
| `HAPPY_WAKEY_API_TCP_MAX_CONNECTIONS` | no | Bounded concurrent connection limit |
| `HAPPY_WAKEY_API_TCP_MAX_REQUESTS_PER_CONNECTION` | no | Reauthentication/frame limit |
| `HAPPY_WAKEY_API_TCP_IDLE_TIMEOUT_SECONDS` | no | Per-frame idle timeout |
| `HAPPY_WAKEY_NATS_URL` | no | Enables async processing; must use `tls://` |
| `HAPPY_WAKEY_NATS_CREDENTIALS_FILE` | with NATS | NKey/JWT credentials file; URL credentials are rejected |
| `HAPPY_WAKEY_NATS_REQUEST_STREAM` | no | Pre-provisioned request stream name |
| `HAPPY_WAKEY_NATS_RESPONSE_STREAM` | no | Pre-provisioned response stream name |
| `HAPPY_WAKEY_NATS_CONSUMER` | no | Pre-provisioned durable pull consumer name |

Cross-repository Cargo dependencies are immutable. This implementation pins
`happy-wakey-interfaces` at
`d6278ec8f6b2263678728b147a32dff92d52d8c8` and ores-otel logging at
`ca176fb6768a9750d262a536952268625ffd3a8a`. The versioned Shared Auth wire
contract implemented by the fail-closed HTTPS adapter was finalized in
`shared-auth-interfaces` at
`e60d862a59828a3690852252adcafaea1266268a`.

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
