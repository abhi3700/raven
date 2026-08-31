# Raven

<p align="center">
  <img src="./res/logo.png" width="180" alt="Raven logo" />
</p>

<p align="center">
  <strong>A programmable blockchain event runtime powered by plugins.</strong>
</p>

<p align="center">
  Build event-driven blockchain applications with normalized chain events,
  a source-independent runtime, and reusable Rust plugins.
</p>

## Why Raven?

Blockchain applications often rebuild the same RPC ingestion, block
normalization, and event dispatch pipeline. Raven separates that infrastructure
from application behavior:

```mermaid
flowchart LR
    chain[EVM chain] --> source[Event source]
    source --> event[Normalized ChainEvent]
    event --> runtime[Raven Runtime]
    runtime --> plugins[Plugins]
```

The RPC source, currently backed by Alloy, owns JSON-RPC access. `raven-core`
owns validated event types, the runtime owns lifecycle and dispatch, and
plugins own application logic.

## Current features

- Event-driven, async Rust architecture
- Validation-preserving construction and deserialization for core events
- Unique-name plugin registration and lifecycle hooks
- Independent plugin workers with bounded FIFO mailboxes
- Non-blocking fan-out, immediate delivery receipts, and live per-plugin outcomes
- Error isolation and panic quarantine without blocking sibling plugins
- Automatic EIP-155 chain discovery with no chain allowlist
- Runtime state and per-chain validation
- Transport-aware JSON-RPC ingestion backed by Alloy: HTTP polling or WS subscriptions with reconciliation
- Validated batched block/log ingestion by default, with hash-pinned sequential compatibility
- Capped transient-RPC retry with jitter and structured telemetry
- Durable per-chain resume, explicit block starts, and bounded shallow-reorg correction
- At-least-once CLI delivery: checkpoints commit only after every plugin succeeds
- Bounded plugin lifecycle hooks, worker health, and aggregate cleanup errors
- Persistent RPC URL configuration with CLI and environment overrides
- Working CLI orchestration with graceful Ctrl+C shutdown
- Persisted bundled-plugin installation and configuration
- Bundled monitoring for large ERC-20 transfers, including reorg corrections
- Bundled reorg monitoring for applied and reverted canonical blocks
- Color-coded plugin identity tags for readable multi-plugin terminal output
- Normalized event boundary designed for future Reth ExEx support

> **Important:** Raven is early-stage. External plugin discovery/loading and
> the Raven-enabled Reth CLI/ExEx integration are planned but not yet implemented.

## Getting started

### Prerequisites

- Rust 1.91 or newer
- Cargo
- An EVM-compatible HTTP(S) or WS(S) JSON-RPC endpoint

### Build and test

```bash
git clone https://github.com/abhi3700/raven.git
cd raven

cargo build --workspace
cargo test --workspace --all-targets
```

### Run with Alloy

Pass an endpoint directly:

```bash
cargo run -p raven -- run \
  --rpc-url https://your-evm-rpc.example
```

Persist the endpoint once and then run without repeating it:

```bash
raven config set --rpc-url http://localhost:8545
raven config get
raven run
```

You can also configure it for the current environment:

```bash
export NODE_RPC_URL="http://localhost:8545"
cargo run -p raven -- run
```

WebSocket endpoints work through persisted config, the option, or the
environment variable:

```bash
raven run --rpc-url wss://your-evm-rpc.example
```

Resolution order is `--rpc-url`, then `NODE_RPC_URL`, then persisted config.
Run `raven config clear` to remove the persisted RPC URL and plugin installations.

Raven calls `eth_chainId`, accepts any non-zero EIP-155 chain ID, selects that
chain's durable checkpoint, and initializes the runtime. There is no
Ethereum-mainnet allowlist. It then fetches blocks and their logs in one
validated JSON-RPC batch by default, sends normalized events through the
runtime, and logs each block with the built-in `block-logger` plugin. Use
`--block-fetch-mode sequential` for the original hash-pinned two-request
strategy. HTTP(S) polls for new
heights; WS(S) subscribes to `newHeads` and periodically reconciles missed
heights. Press Ctrl+C to shut down cleanly.

`raven run` defaults to `--start resume`: it continues after the last event
successfully handled by every plugin, or starts at the current head when there
is no checkpoint. Use `--start latest` to ignore saved progress or
`--start <BLOCK>` to backfill from an inclusive height. Raven retains 64 recent
canonical blocks by default and delivers shallow-fork reverts before
replacement blocks.

Install the bundled ERC-20 transfer monitor with an inclusive threshold in raw
token units, then start Raven with no plugin-specific run flags:

```bash
raven plugins install erc20-transfer \
  --min-amount 1000000000000000000000 \
  --token 0x1111111111111111111111111111111111111111 0x2222222222222222222222222222222222222222
raven run
```

List several contracts after one `--token` flag, or omit it to accept valid
ERC-20 `Transfer` logs from every contract. One minimum amount applies to every
listed token; provide the same number of amounts and addresses for positional
thresholds. Long terminal output is the default; add `--short` during
installation to abbreviate hashes and addresses. Reinstalling the same plugin
updates its saved configuration.

Install the reorg monitor to report every applied/reverted transition from the
same normalized event stream:

```bash
raven plugins install reorg-monitor
raven plugins list --details
raven run
```

Applied blocks log at `INFO`; reverted blocks log at `WARN` with their hashes,
so replacement blocks at the same heights remain distinguishable. The monitor
does not make RPC calls or infer forks itself. It observes the source's bounded
shallow-reorg correction sequence: old-tip reverts first, then replacement
applies.

When multiple plugins are enabled, events emitted inside each plugin's
lifecycle are prefixed with a color-coded `[ plugin-name ]` terminal tag. Raven
assigns colors as plugins are registered, so the tag makes interleaved block,
transaction, and log observations attributable without changing plugin code.
The plain tag remains when `NO_COLOR` is set or output is redirected.

One Raven process handles one discovered chain so event ordering and plugin
state cannot accidentally cross chains. Run separate Raven processes to ingest
multiple EVM chains concurrently.

Tune the polling interval when needed:

```bash
cargo run -p raven -- run \
  --rpc-url http://localhost:8545 \
  --poll-interval-ms 1000
```

For WS(S), tune the lower-frequency safety reconciliation independently:

```bash
raven run \
  --rpc-url wss://your-evm-rpc.example \
  --reconciliation-interval-ms 30000
```

Use `cargo run -p raven -- --help` for the full CLI help.

`raven run` always means standalone JSON-RPC ingestion, so it has no
`--source alloy` option. Whether the endpoint is a hosted provider or your own
local node does not change this data path; the URL scheme selects HTTP polling
or WebSocket subscription behavior.

Future Reth ExEx support has a different deployment model. An ExEx is compiled
into and launched with a Reth node, so Raven plans a version-pinned
Raven-enabled Reth CLI. Operators will run the normal `reth node` flow with
Raven/plugin configuration, and the node builder will install Raven in-process.
It will not pretend to attach through a `--source reth` flag.

## Write a plugin

```rust
use async_trait::async_trait;
use raven_core::ChainEvent;
use raven_plugin_sdk::{
    Plugin, PluginContext, PluginMetadata, PluginResult,
};

struct BlockLogger;

#[async_trait]
impl Plugin for BlockLogger {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata::new(
            "block-logger",
            env!("CARGO_PKG_VERSION"),
            "Logs newly applied blocks",
        )
    }

    async fn handle_event(
        &mut self,
        event: &ChainEvent,
        _context: &PluginContext,
    ) -> PluginResult {
        println!("block #{}", event.block_number());
        Ok(())
    }
}
```

Run the self-contained SDK example:

```bash
cargo run -p raven-plugin-sdk --example block_logger
```

## Workspace structure

```text
raven/
├── CHANGELOG.md            # Weekly commit-derived project history
├── crates/
│   ├── cli/                 # CLI and source/runtime orchestration
│   ├── core/                # Validated normalized chain events
│   ├── plugins/             # Bundled, statically linked plugin crates
│   ├── plugin-sdk/          # Plugin contract and context
│   ├── runtime/             # Registry, lifecycle, and dispatcher
│   └── source-alloy/        # Alloy-backed HTTP/WebSocket JSON-RPC source
├── docs/                    # Mintlify MDX pages
├── docs.json                # Mintlify site configuration
├── res/                     # Brand assets
└── README.md
```

## Documentation

The complete Mintlify documentation starts at
[docs/index.mdx](./docs/index.mdx). Its navigation and theme are configured in
[docs.json](./docs.json).

The [design principles](./docs/concepts/design-principles.mdx) reconcile the
project vision with current architectural boundaries.

Development history is summarized by UTC ISO week in the
[changelog](./CHANGELOG.md), with links back to every source commit.

Preview the site locally with Node.js 20.17 or newer:

```bash
npm install -g mint
./doc.sh
```

The preview uses port `3777` by default. Override it or pass additional
Mintlify flags when needed:

```bash
MINTLIFY_PORT=4000 ./doc.sh --no-open
```

Before publishing documentation changes:

```bash
mint broken-links
mint validate
```

## Roadmap

The reliability milestone is complete. The next product milestone is the first
production-shaped plugin, while Reth integration can proceed against the now
explicit acknowledgement contract:

- A large ERC-20 transfer plugin as the first vertical slice
- Typed plugin configuration and a production output sink
- A `raven-source-reth` adapter for commit/revert/reorg notifications
- A version-pinned Raven-enabled Reth CLI that installs the ExEx during
  `reth node` launch

See the [full roadmap](./docs/reference/roadmap.mdx) for implemented and planned
capabilities, exit criteria, and later Reth/plugin-platform work.

## License

MIT OR Apache-2.0
