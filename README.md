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
- Capped transient-RPC retry with jitter and structured telemetry
- Durable per-chain resume, explicit block starts, and bounded shallow-reorg correction
- At-least-once CLI delivery: checkpoints commit only after every plugin succeeds
- Bounded plugin lifecycle hooks, worker health, and aggregate cleanup errors
- Persistent RPC URL configuration with CLI and environment overrides
- Working CLI orchestration with graceful Ctrl+C shutdown
- Normalized event boundary designed for future Reth ExEx support

> **Important:** Raven is early-stage. Dynamic plugin installation and the
> Raven-enabled Reth CLI/ExEx integration are planned but not yet implemented.

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
Run `raven config clear` to remove the persisted value.

Raven calls `eth_chainId`, accepts any non-zero EIP-155 chain ID, selects that
chain's durable checkpoint, and initializes the runtime. There is no
Ethereum-mainnet allowlist. It
then fetches full blocks, sends normalized events through the runtime, and logs
each block with the built-in `block-logger` plugin. HTTP(S) polls for new
heights; WS(S) subscribes to `newHeads` and periodically reconciles missed
heights. Press Ctrl+C to shut down cleanly.

`raven run` defaults to `--start resume`: it continues after the last event
successfully handled by every plugin, or starts at the current head when there
is no checkpoint. Use `--start latest` to ignore saved progress or
`--start <BLOCK>` to backfill from an inclusive height. Raven retains 64 recent
canonical blocks by default and delivers shallow-fork reverts before
replacement blocks.

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
│   ├── raven-cli/           # CLI and source/runtime orchestration
│   ├── raven-core/          # Validated normalized chain events
│   ├── raven-plugin-sdk/    # Plugin contract and context
│   ├── raven-runtime/       # Registry, lifecycle, and dispatcher
│   └── raven-source-alloy/  # Alloy-backed HTTP/WebSocket JSON-RPC source
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
