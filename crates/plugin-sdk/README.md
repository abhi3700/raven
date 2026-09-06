# Raven Plugin SDK

This crate defines Raven's source-independent `Plugin` contract, metadata,
execution context, and plugin errors.

## Plugin lifecycle

```text
Raven starts
    │
    ▼
plugin.start()
    │
    ▼
plugin.handle_event()
    │
    ▼
plugin.handle_event()
    │
    ▼
plugin.shutdown()
```

`handle_event` is the only required lifecycle method. Plugins should consume
normalized `ChainEvent` values and leave chain ingestion to source adapters.

Run the self-contained example from the workspace root:

```bash
cargo run -p raven-plugin-sdk --example block_logger
```

See the full [plugin guide](../../docs/guides/create-a-plugin.mdx).
