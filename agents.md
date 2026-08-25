# Happy Wakey API instructions

## Authority boundaries

- `happy-wakey-interfaces` is the canonical wire-contract authority. Do not
  fork request, response, transition, or sync contracts locally.
- Shared Auth is the sole identity authority. Keep introspection fail-closed,
  require HTTPS, and do not follow redirects.
- `reducer::decide` is the sole authority for occurrence transitions. Invalid
  and stale transitions must return without mutating occurrence state.
- Keep idempotency receipt claims, state mutations, and receipt finalization in
  one database transaction.
- Scope every customer read and write to the verified Shared Auth subject.
- Never run DDL or migrations at application startup.

## Dependency management

- Use the repository `.zpkg.toml` and released `zed-pkg` CLI for dependency
  resolution and installation. Do not add unpinned cross-repository Git heads.
- Keep Cargo Git revisions immutable and update the Zed dependency declaration
  in the same review when a cross-repository dependency changes.

## Validation

- Run `zed validate` and `zed install --adapter rust` before native checks.
  Commit the generated lock only when every entry has a portable immutable
  registry source; never commit a workstation-local `file://` source.
- Run `zed run cargo test --locked`, `zed run cargo fmt --all -- --check`, and
  `zed run cargo clippy --all-targets --locked -- -D warnings`.
- Search for unresolved conflict markers before handoff.
