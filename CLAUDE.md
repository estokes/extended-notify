# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
cargo build              # Build the library
cargo test               # Run all tests
cargo test <test_name>   # Run a single test (e.g., cargo test watch_existing_file)
cargo check              # Type check without building
```

Enable debug logging in tests with `RUST_LOG=debug cargo test`.

## Architecture

This is a Rust library that extends the `notify` crate with support for watching non-existent paths, interest-based filtering, and RAII handles.

### Core Components

- **`lib.rs`** - Public API: `Watcher`, `Watched` (RAII handle), `Interest` (bitflags enum), `ArcPath`, `EventHandler` trait
- **`watch_task.rs`** - Background task that coordinates between notify events, command channel, and polling

### Key Design Decisions

**Non-existent path handling**: When watching a path that doesn't exist, the library watches the nearest existing ancestor and polls to detect creation. `PathStatus` tracks the canonical existing portion vs missing parts.

**Interest filtering**: `Interest` is a hierarchical bitflag enum. Parent interests (e.g., `Delete`) match child events (`DeleteFile`, `DeleteFolder`). The `Watch::interested()` method handles this hierarchy.

**Synthetic events**: `Established` fires immediately when a watch is set up. Synthetic `Create`/`Delete` events are generated when paths transition between existing and non-existing states.

**Reference counting**: Multiple watches on the same path share a single underlying notify watch via `by_root` map in the `Watched` struct.

### Event Flow

1. User calls `Watcher::add()` → sends `Cmd::Watch` to background task
2. Background task determines if path exists, sets up notify watch on existing ancestor
3. Events from notify + poll results flow through `Watched::process_event()`
4. Events are batched and delivered via `EventHandler::handle_event()`

### Test Framework

`test.rs` provides a declarative test DSL with `Step` enum (Watch, CreateFile, DeleteFile, etc.) and `Expectation` enum for verifying events. Use `Exactly`, `AtLeast`, or `ExactlyWithOptional` to assert on received events.
