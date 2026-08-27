# Repository Guidelines

## Scope

These instructions apply to the entire Raven repository.

Raven is a Rust workspace for a programmable blockchain event runtime powered by
plugins. Keep implementation, documentation, and roadmap language aligned with
what the repository actually supports today.

## Architecture Boundaries

- `crates/raven-core` owns source-independent normalized chain types, validation,
  and core errors.
- `crates/raven-runtime` owns plugin registration, lifecycle, dispatch,
  worker isolation, delivery outcomes, and runtime health.
- `crates/raven-plugin-sdk` owns the public plugin contract, context, metadata,
  and example plugin surface.
- `crates/raven-source-alloy` owns Alloy-backed HTTP/WebSocket JSON-RPC
  ingestion, source-native conversion, retry/reconciliation, and reorg emission.
- `crates/raven-cli` owns Clap UX, config resolution, source/runtime
  orchestration, checkpoints, terminal output, logging, and shutdown handling.

Do not move behavior across these boundaries without a clear reason. In
particular, core types should stay source-independent, while Alloy-specific RPC
logic should stay in `raven-source-alloy`.

## Rust Conventions

- Prefer established EVM primitives and parsers from Alloy, especially
  `alloy-primitives`, for hashes, addresses, fixed bytes, quantities, and hex
  parsing. Do not hand-roll validation when an Alloy primitive expresses the
  contract correctly.
- Preserve Raven's public contracts even when a library parser is more
  permissive. For example, if docs or serialized events require `0x`-prefixed
  hashes, keep that explicit rule at the Raven boundary.
- Use typed errors at crate boundaries. Keep user-facing CLI diagnostics in the
  CLI crate rather than leaking internal formatting decisions from lower crates.
- Avoid broad refactors while changing behavior. Keep edits scoped to the
  module and crate that own the feature.
- Add tests for validation logic, state transitions, checkpoint behavior,
  dispatch guarantees, and any public CLI parsing changes.

## Documentation

- If required, also update the doc whenever anything is added.
- Update `README.md`, `docs/`, and `docs.json` when a code change adds,
  removes, renames, or materially changes user-visible behavior.
- Keep docs synchronized with implemented behavior. Clearly label planned Reth
  ExEx integration and dynamic plugin installation as planned unless they are
  implemented in the current tree.
- Use the repository term `Plugin` for the extension contract. Avoid replacing
  it with `processor` unless the code architecture changes.
- Keep Mintlify routes and navigation valid when adding or moving docs.

## CLI And Runtime Behavior

- `raven run` is standalone JSON-RPC ingestion through the Alloy-backed source.
  The URL scheme selects HTTP polling or WebSocket subscription behavior.
- RPC URL resolution is: explicit `--rpc-url`, then `NODE_RPC_URL`, then
  persisted config.
- Checkpoints are CLI-owned durable acknowledgement state. They advance only
  after every accepted plugin outcome succeeds, and they provide at-least-once
  recovery rather than exactly-once plugin side effects.
- A checkpoint's block window is canonical ancestry, ordered oldest to newest.
  Applied blocks must extend the tip; reverted blocks must exactly match and
  remove the tip.
- Keep `CHECKPOINT_SCHEMA_VERSION` as the persisted-format compatibility marker.
  Fail closed on unknown versions and add migration/compatibility before
  accepting older incompatible files.

## Validation

Use the narrowest command that proves the change, then broaden when behavior
crosses crate boundaries.

Recommended checks:

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
git diff --check
```

For documentation changes, also run:

```bash
mint validate
mint broken-links
```

If Mintlify fails due to user-level cache permissions in the sandbox, report
that explicitly instead of claiming docs validation passed.

## Git Hygiene

- The worktree may contain user changes. Do not revert unrelated changes.
- Before editing, inspect the files you will touch and keep the diff scoped.
- Do not run destructive git commands unless the user explicitly requested them.
- Report validation honestly, including commands that were skipped or blocked.
