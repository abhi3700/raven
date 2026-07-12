# Raven

<p align="center">
  <img src="./res/logo.png" width="180" alt="Raven Logo">
</p>

<p align="center">
  <strong>A programmable event engine for EVM chains.</strong>
</p>

<p align="center">
  Build event-driven blockchain applications using modular plugins, powered by Alloy today and Reth ExEx tomorrow.
</p>

---

## Why Raven?

Today, most blockchain applications continuously poll RPC endpoints to detect new blocks, transactions, and logs.

```
  RPC
   │
   ▼
Your Backend
   │
Polling
   │
Database
```

This approach works, but every application ends up rebuilding the same ingestion pipeline.

Raven takes a different approach.

```
Blockchain
      │
      ▼
Event Source
(Alloy / Reth)
      │
      ▼
Raven Runtime
      │
      ▼
Plugins
      │
      ├── Portfolio
      ├── Swap Analytics
      ├── Whale Alerts
      ├── Telegram
      ├── Database
      └── ...
```

Instead of building another indexer, Raven provides a runtime for composing reusable blockchain event processors.

---

## Features

- ⚡ Event-driven architecture
- 🔌 Modular plugin system
- 🦀 Written in Rust
- 🌐 Multi-chain EVM support
- 🔄 Source abstraction (Alloy today, Reth ExEx later)
- 📦 CLI-first experience
- 🚀 Async runtime powered by Tokio

---

## Architecture

```
                 Raven CLI
                      │
                      ▼
               Raven Runtime
                      │
          ┌───────────┴────────────┐
          │                        │
     Alloy Source             Reth Source
      (RPC/WebSocket)          (ExEx)
          │                        │
          └───────────┬────────────┘
                      ▼
              Chain Event Stream
                      │
              Event Dispatcher
                      │
      ┌───────────────┼────────────────┐
      ▼               ▼                ▼
  Swap Plugin   Portfolio Plugin   Whale Plugin
                      │
                      ▼
                 Output Sinks
```

---

## Project Structure

```
raven/
├── crates/
│   ├── raven-cli/
│   ├── raven-core/
│   ├── raven-plugin-sdk/
│   └── raven-source-alloy/
│
├── plugins/
│
├── examples/
│
└── README.md
```

---

## Getting Started

<!-- TODO: -->

### Prerequisites

Before running Raven, ensure you have:

- Rust (stable)
- Cargo
- An Ethereum RPC endpoint (e.g. local node, LlamaRPC, Alchemy, QuickNode)

Verify your Rust installation:

```sh
rustc --version
cargo --version
```

### Clone the repository

```sh
git clone https://github.com/abhi3700/raven.git
cd raven
```

### Build Raven

```sh
cargo build
```

For a release build:

```sh
cargo build --release
```

### Run Raven

Follow the [usage](#usage).

## Usage

> [!NOTE]
> If using cargo to run the `raven`, then use `cargo r -p raven -- COMMAND ...`. If `raven` installed using `cargo install ..`, then use like `raven COMMAND ...`.

### Start Raven with Alloy

```bash
cargo r -p raven -- run \
  --source alloy \
  --rpc-url https://ethereum-rpc.publicnode.com
```

Raven connects to the configured RPC endpoint, listens for new blocks, converts them into `ChainEvent`s, and dispatches them to all enabled plugins.

---

### List installed plugins

```bash
raven plugins list
```

Example output:

```text
✔ transfer
✔ swap
✔ portfolio
```

---

### Install a plugin *(planned)*

```bash
raven plugins install whale
```

---

### Run with Reth *(planned)*

```bash
raven run \
  --source reth
```

When using Reth, Raven consumes execution events directly from a local Reth node via ExEx instead of an RPC endpoint.

---

### Show configuration

```bash
raven config
```

---

### Verify your installation

```bash
raven doctor
```

## Philosophy

Raven is built around one idea:

> **Everything is an event.**

A new block.

A transfer.

A swap.

A liquidation.

A contract deployment.

Every event flows through the runtime, where plugins decide how to react.

---

## Example

```rust
#[async_trait]
impl Plugin for SwapPlugin {
    async fn on_event(
        &mut self,
        event: &ChainEvent,
        ctx: &Context,
    ) -> Result<()> {
        // Decode swaps
        // Update metrics
        // Store data
        Ok(())
    }
}
```

---

## Planned Plugins

- ERC20 Transfers
- ERC721 Events
- DEX Swap Decoder
- Portfolio Tracker
- Whale Tracker
- Telegram Alerts
- PostgreSQL Sink
- SQLite Sink
- Redis Sink
- Kafka Sink
- Webhook Sink

---

## Event Sources

### Alloy (Phase 1)

```bash
raven run \
  --source alloy \
  --rpc-url https://...
```

Ideal for development and existing RPC providers.

---

### Reth ExEx (Phase 2)

```bash
raven run \
  --source reth
```

Runs directly on top of a local Reth node for low-latency, execution-aware event processing.

---

## Roadmap

### Phase 1

- [ ] CLI
- [ ] Alloy event source
- [ ] Event dispatcher
- [ ] Plugin SDK
- [ ] Transfer plugin

### Phase 2

- [ ] Swap decoder
- [ ] Portfolio plugin
- [ ] SQLite sink
- [ ] PostgreSQL sink

### Phase 3

- [ ] Plugin installation
- [ ] Plugin registry
- [ ] Dynamic loading
- [ ] Webhook support

### Phase 4

- [ ] Reth ExEx integration
- [ ] Reorg-aware processing
- [ ] Distributed runtime

---

## License

MIT OR Apache-2.0
