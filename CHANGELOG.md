# Changelog

This changelog summarizes Raven's committed development history by ISO calendar
week. Weeks run from Monday through Sunday, all commit timestamps and calendar
groupings are normalized to UTC, and the newest week appears first.

The dated weekly history includes only committed work. Exact commit titles and
links are retained under each active week so every summary can be traced back
to Git history; `Unreleased` tracks current changes before their first release.
This snapshot covers the repository from its inception through commit
[`12ad8b7`](https://github.com/abhi3700/raven/commit/12ad8b7f72812d639db08df8c826c1cbc10b9821)
on August 25, 2026 (UTC).

## Unreleased

### Added

- Added typed, validated EVM logs to normalized block events. The Alloy source
  fetches blocks and logs in one consistency-checked JSON-RPC batch by default,
  preserves hash-pinned sequential retrieval as an explicit mode, and retains
  logs for matching reorg reversals.
- Added the statically linked `erc20-transfer` plugin with shared or positional
  per-token raw-unit thresholds, optional token-contract filters, correction
  telemetry, deterministic tests, and an opt-in live RPC example.
- Added a dedicated Plugins documentation section, starting with the ERC-20
  transfer plugin's configuration, matching, reorg, and delivery semantics.
- Added a concise contribution guide covering Raven fundamentals, plugin
  creation and tests, documentation, validation, pull requests, and review.

### Changed

- Replaced string block and parent hashes with `alloy_primitives::B256` while
  preserving their `0x`-prefixed JSON representation.
- Extended `raven run` with `--erc20-transfer-min-amount` and repeatable
  `--erc20-token` options.
- Updated `raven plugins list` to show disabled built-in plugins with their
  enable command, starting with `erc20-transfer`.
- Added the opt-in `reorg-monitor` built-in plugin, which reports normalized
  block applies and shallow-reorg reverts through tracing.
- Tagged plugin-originated terminal logs with deterministic, distinct colors
  assigned during CLI registration, while preserving plain tags without ANSI.
- Established `crates/plugins/<plugin>/` as the workspace layout for static
  plugin crates and moved `raven-plugin-erc20-transfer` into it.

## 2026-W35 (August 24-30, 2026)

### Added

- Introduced one isolated asynchronous worker and bounded FIFO mailbox per
  plugin. Event fan-out now returns immediate per-plugin delivery receipts,
  while plugin results arrive independently without a slow plugin delaying its
  siblings.
- Added per-plugin outcome reporting, overload and closed-worker reporting,
  panic quarantine, concurrent startup, and graceful worker shutdown.
- Added WebSocket RPC support. WS(S) sources now use `eth_subscribe` with
  `newHeads`, while an ordered cursor fetches full blocks and periodic
  reconciliation recovers missed notifications.
- Added persistent CLI configuration with `raven config set --rpc-url`,
  `raven config get`, and `raven config clear`.
- Made chain handling EVM-chain-agnostic. Raven discovers any non-zero EIP-155
  chain ID, binds each runtime process to that chain, and prevents events from
  a different chain from entering the same plugin state.

### Changed

- Documented the runtime's high-level and low-level concurrency design,
  per-plugin ordering, backpressure, error isolation, and transport behavior.
- Refined CLI output and adopted the caption "A programmable blockchain event
  runtime powered by plugins."
- Removed the implementation-specific `--source` option and documented the
  standalone RPC versus embedded Reth deployment boundary.
- Added colored command output and type-aware ANSI coloring for structured log
  values, with plain output when color is disabled.
- Normalized this changelog's commit timestamps and week grouping to UTC.
- Hardened normalized-event validation, Alloy conversion, polling validation,
  and runtime outcome logging and tests.

<details>
<summary>Commits (8)</summary>

- [`c20b00d`](https://github.com/abhi3700/raven/commit/c20b00d4ec3ca98c0542e6907ac382c62fb17338) - feat: add isolated concurrent plugin workers
- [`d6d1c6f`](https://github.com/abhi3700/raven/commit/d6d1c6ff4bb62bf7d13a3b108ac5b424f8dd737f) - support ws rpc url; replace caption for raven
- [`b0da527`](https://github.com/abhi3700/raven/commit/b0da527aa8ae73359aa1aad35e6901ffca9c2e49) - make websocket connect via eth_subscribe + reconciliation instead of polling like in http
- [`b9bbfb4`](https://github.com/abhi3700/raven/commit/b9bbfb4477e55952e852811ff9ff5b049f2ff882) - add config command to set rpc-url to a file, get config, clear config
- [`298cc6a`](https://github.com/abhi3700/raven/commit/298cc6ae3cdddff2d04a17774c63dd4c2e931e4d) - Implemented chain-agnostic EVM support.
- [`aa910ec`](https://github.com/abhi3700/raven/commit/aa910ec47e2fb40559cdfd898502f4ce9da83291) - add changelog as per UTC
- [`5da124c`](https://github.com/abhi3700/raven/commit/5da124c6f4dc29b944fda974ece392853cd2b3bc) - removed --source flag altogether in CLI
- [`12ad8b7`](https://github.com/abhi3700/raven/commit/12ad8b7f72812d639db08df8c826c1cbc10b9821) - add colors to CLI

</details>

## 2026-W34 (August 17-23, 2026)

### Added

- Added the Mintlify documentation site with getting-started guides,
  architecture and event concepts, plugin and Alloy guides, CLI reference, and
  a phased roadmap.
- Added local documentation preview and validation tooling.

### Changed

- Refactored the CLI into parsing and runner/orchestration modules and connected
  the Alloy source, runtime, and built-in block logger into a working command.
- Reworked the README and documentation to separate implemented behavior from
  planned functionality and to consistently describe Raven as a plugin system.
- Integrated the Phase 1 development branch.

<details>
<summary>Commits (3)</summary>

- [`02cbd46`](https://github.com/abhi3700/raven/commit/02cbd465033951762be0790d003bfc3bc4d5e60d) - improve doc, code refactor
- [`8d24fa2`](https://github.com/abhi3700/raven/commit/8d24fa22794cae7bbd05c4c64a81677286e48234) - Merge branch 'feat/phase-1'
- [`c0d6b87`](https://github.com/abhi3700/raven/commit/c0d6b8773103f9c248b765e5a8dd66cffd3ec9c5) - update docs

</details>

## 2026-W33 (August 10-16, 2026)

- No repository commits were recorded this week.

## 2026-W32 (August 3-9, 2026)

- No repository commits were recorded this week.

## 2026-W31 (July 27-August 2, 2026)

- No repository commits were recorded this week.

## 2026-W30 (July 20-26, 2026)

- No repository commits were recorded this week.

## 2026-W29 (July 13-19, 2026)

### Added

- Added the Alloy event-source crate with JSON-RPC ingestion, event conversion,
  error handling, and an end-to-end example.

<details>
<summary>Commits (1)</summary>

- [`cf7feab`](https://github.com/abhi3700/raven/commit/cf7feab31b6bf1ca32de2de84b11dc5c3ca3bce0) - add crate for raven-source alloy

</details>

## 2026-W28 (July 6-12, 2026)

### Added

- Established the project license and Raven logo.
- Initialized the Rust workspace with CLI, core, plugin SDK, and Alloy source
  crate scaffolding, plus shared toolchain and formatting configuration.
- Added the first README describing Raven's purpose and intended architecture.
- Implemented `raven-core` with chain, block, event, and error domain types.
- Implemented the plugin SDK contract, including plugin metadata, context,
  lifecycle and event hooks, errors, documentation, and a block-logger example.
- Added `raven-runtime` with plugin registration, lifecycle management, event
  dispatch, errors, and registry debug output.

### Changed

- Expanded the README to describe the initial architecture and development
  direction.

<details>
<summary>Commits (9)</summary>

- [`6ee4bca`](https://github.com/abhi3700/raven/commit/6ee4bca8f971de4646601cc053bf4c51677edc51) - Initial commit
- [`893d713`](https://github.com/abhi3700/raven/commit/893d713b21206e5dd9ed20b2c2e2be2fec6f986c) - add logo
- [`6ed9d84`](https://github.com/abhi3700/raven/commit/6ed9d845de8c7a8f9a8d7d1bafc74793bc3739f1) - chore: initialize Raven workspace
- [`f060f76`](https://github.com/abhi3700/raven/commit/f060f76c4d8989bfde3ca07fbea92f1754c2540f) - add README
- [`3db8132`](https://github.com/abhi3700/raven/commit/3db81328b8e6d3e8cd523aadb1d84e9e055d62b7) - update readme
- [`df4a32c`](https://github.com/abhi3700/raven/commit/df4a32cf10d79d0b61a4aaaab8773ff249d97bfe) - add raven-core initial code
- [`6379ddf`](https://github.com/abhi3700/raven/commit/6379ddf17df0d93ae3695c4ebb9a7a0b6c3fd8ee) - Add code for raven plugin SDK
- [`c699caf`](https://github.com/abhi3700/raven/commit/c699caf44874a120cd764c6b74a83d5ab5d0074e) - Add code for rave-runtime
- [`02f1198`](https://github.com/abhi3700/raven/commit/02f11987c1c70d9994619c4a427d7be590485f8e) - Add Debug trait for PluginRegistry

</details>
