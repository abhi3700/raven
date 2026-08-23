# Raven Plugin SDK

## Plugin lifecycle

```sh
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
