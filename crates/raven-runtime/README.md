# Raven Runtime

This is the heart of Raven, while the CLI & event sources are just ways to interact with it.

Owns everything

```rust
pub struct Runtime {
    dispatcher: Dispatcher,
}
```

## Dispatcher

Receives

```rust
ChainEvent
```

and forwards it to every registered plugin.

```sh
ChainEvent

↓

Dispatcher

↓

Plugin A

Plugin B

Plugin C
```

## Registry

Stores plugins.

Initially something like

```rust
Vec<Box<dyn Plugin>>
```

Later you can support:

- enable/disable
- priorities
- dependencies
- plugin IDs

without changing the dispatcher.

## Error

Runtime-specific errors only.

---

## Conclusion

Then Alloy becomes tiny.

Once the runtime exists, Alloy only does one job.

```sh
watch_blocks()

↓

convert to ChainEvent

↓

runtime.dispatch(event)
```

That’s all.

---

Similarly for Reth, it becomes:

```sh
ExEx Notification

↓

convert to ChainEvent

↓

runtime.dispatch(event)
```

No plugin changes.

No runtime changes.

No dispatcher changes.

---

Final Architecture

```sh
              CLI
               │
               ▼
            Runtime
               │
           Dispatcher
               ▲
               │
        ChainEvent
        ▲        ▲
        │        │
    Alloy      Reth
```

> Notice how `Alloy` and `Reth` are now just adapters.

That’s exactly what we want.
