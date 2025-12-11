# extended-notify

A filesystem watcher built on [notify](https://crates.io/crates/notify) that adds support for watching paths that don't yet exist, interest-based event filtering, and automatic cleanup via RAII handles.

## Features

- **Watch non-existent paths**: Subscribe to paths before they exist. When the path is created, you'll receive events.
- **Interest filtering**: Specify exactly which event types you care about with fine-grained or coarse-grained filters.
- **RAII handles**: Watches automatically stop when the `Watched` handle is dropped.
- **Polling fallback**: Configurable polling covers edge cases that native filesystem notifications miss.
- **Batched async delivery**: Events are batched and delivered through an async `EventHandler` trait.

## Example

```rust
use extended_notify::{ArcPath, EventBatch, EventHandler, Interest, Watcher};
use anyhow::Result;
use enumflags2::make_bitflags;

struct MyHandler;

impl EventHandler for MyHandler {
    async fn handle_event(&mut self, batch: EventBatch) -> Result<()> {
        for (id, event) in batch.iter() {
            println!("{:?}: {:?}", id, event);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let watcher = WatcherConfigBuilder::default()
        .event_handler(MyHandler)
        .build()?
        .start()?;

    // Watch a path that may or may not exist
    let interest = make_bitflags!(Interest::{Create | Delete | Modify});
    let handle = watcher.add(ArcPath::from("/tmp/watched-file"), interest)?;

    // ... do work ...

    // Watch stops automatically when handle is dropped
    drop(handle);
    Ok(())
}
```

## Interest Filtering

Interests are hierarchical. Subscribing to a parent interest matches all child events:

- `Delete` matches `DeleteFile`, `DeleteFolder`, `DeleteOther`
- `Modify` matches all modification types (data, metadata, rename)
- `Any` matches everything

Special interests:
- `Established`: Synthetic event fired when a watch is first set up
- `Any`: Matches all event types

## How It Works

When you watch a path:

1. If the path exists, it's watched directly via notify
2. If not, the nearest existing ancestor is watched, and polling checks for the path's creation

When a watched path is deleted or renamed away, the watcher automatically transitions back to watching the ancestor and polling. Synthetic `Create` and `Delete` events are generated for these transitions if you've subscribed to them.

## Configuration

```rust
// Adjust polling interval (default: 1s, minimum: 100ms)
watcher.set_poll_interval(Duration::from_secs(5))?;

// Adjust poll batch size (default: 100, 0 disables polling)
watcher.set_poll_batch(50)?;
```

## License

MIT
