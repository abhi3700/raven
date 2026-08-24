# Raven Runtime

`raven-runtime` is Raven's orchestration boundary. It validates normalized
events, owns plugin lifecycle, and fans each event out to independent plugin
workers.

```text
Runtime::process(event)
        |
        v
  non-blocking dispatcher
     |          |
     v          v
 mailbox A   mailbox B       <- bounded FIFO queues
     |          |
     v          v
 worker A    worker B        <- independent Tokio tasks
     \          /
      v        v
    live PluginOutcome stream
```

Every worker exclusively owns its `Box<dyn Plugin>`. Events are sequential and
ordered within one plugin, while different plugins execute concurrently.

## Two-stage results

`Runtime::process` returns a `DispatchReceipt` immediately after `try_send` has
been attempted for every plugin. Each delivery is either accepted or explicitly
rejected because the mailbox is full or the worker stopped.

`Runtime::subscribe_outcomes` provides each plugin's later `Succeeded`,
`Failed`, `Panicked`, or `Rejected` result as soon as it exists. A returned
plugin error does not stop its worker or any sibling. A panic quarantines only
the panicked worker.

## Lifecycle

- Startup hooks run concurrently behind one readiness barrier.
- Event handlers run concurrently across plugins and FIFO within each plugin.
- Shutdown closes all mailboxes first, then workers concurrently drain accepted
  events and run their cleanup hooks.

The live broadcast outcome stream is not durable. Per-plugin timeouts, restart
supervision, and persistent replay/dead-letter queues remain future work.

The full HLD and LLD are in
[`docs/concepts/architecture.mdx`](../../docs/concepts/architecture.mdx).
