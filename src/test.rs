#![cfg(test)]

use crate::{ArcPath, EventBatch, EventKind, Id, Interest, Watcher};
use anyhow::{bail, Result};
use enumflags2::BitFlags;
use fxhash::FxHashMap;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::{fs, sync::mpsc, time::timeout};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
#[allow(unused)]
pub enum Expectation {
    /// Expect a single event (current "at least" semantics - extras OK)
    Event { watch: &'static str, kind: Interest, path: &'static str },
    /// Expect an error event on a watch
    Error { watch: &'static str },
}

/// A step in a test sequence
#[derive(Debug, Clone)]
#[allow(unused)]
pub enum Step {
    /// Create a watch with a symbolic name
    Watch { name: &'static str, path: &'static str, interest: BitFlags<Interest> },
    /// Drop a watch by symbolic name
    Unwatch { name: &'static str },
    /// Create a file (parent dirs created automatically)
    CreateFile { path: &'static str },
    /// Create a directory (parent dirs created automatically)
    CreateDir { path: &'static str },
    /// Write content to a file (creates if needed)
    WriteFile { path: &'static str, content: &'static str },
    /// Delete a file
    DeleteFile { path: &'static str },
    /// Delete a directory
    DeleteDir { path: &'static str },
    /// Rename/move a file or directory
    Rename { from: &'static str, to: &'static str },
    /// Sleep for a duration
    Sleep { ms: u64 },
    /// Expect all listed events exist (extras are allowed)
    AtLeast(&'static [Expectation]),
    /// Expect exactly these events and no others
    Exactly(&'static [Expectation]),
    /// Expect exactly the required set, accept the optional set if
    /// it's present, fail if anything else arrives
    ExactlyWithOptional {
        required: &'static [Expectation],
        optional: &'static [Expectation],
    },
    /// Drain any pending events (useful before next action)
    Drain,
}

use Step::*;

struct TestContext {
    temp_dir: TempDir,
    watcher: Watcher,
    rx: mpsc::Receiver<EventBatch>,
    watches: FxHashMap<&'static str, (Id, crate::Watched)>,
    pending_events: Vec<(Id, Interest, ArcPath)>,
    pending_errors: Vec<Id>,
}

impl TestContext {
    async fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        eprintln!("running in {}", temp_dir.path().display());
        let (tx, rx) = mpsc::channel(100);
        let watcher = Watcher::new(tx)?;
        Ok(Self {
            temp_dir,
            watcher,
            rx,
            watches: FxHashMap::default(),
            pending_events: Vec::new(),
            pending_errors: Vec::new(),
        })
    }

    fn resolve_path(&self, path: &str) -> PathBuf {
        self.temp_dir.path().join(path)
    }

    fn process_batch(&mut self, batch: EventBatch) {
        for (id, event) in batch.iter() {
            match &event.event {
                EventKind::Event(interest) => {
                    for path in event.paths.iter() {
                        self.pending_events.push((*id, *interest, path.clone()));
                    }
                }
                EventKind::Error(_) => {
                    self.pending_errors.push(*id);
                }
            }
        }
    }

    async fn recv_events(&mut self) -> Result<()> {
        let wait = Duration::from_millis(500);
        let ts = Instant::now();
        // Wait for at least one batch
        while let Ok(Some(batch)) = timeout(wait - ts.elapsed(), self.rx.recv()).await {
            self.process_batch(batch);
        }
        Ok(())
    }

    fn find_event(
        &mut self,
        watch_id: Id,
        kind: Interest,
        path: &PathBuf,
    ) -> Option<usize> {
        self.pending_events
            .iter()
            .position(|(id, k, p)| *id == watch_id && *k == kind && p.as_path() == path)
    }

    fn find_error(&mut self, watch_id: Id) -> Option<usize> {
        self.pending_errors.iter().position(|id| *id == watch_id)
    }

    async fn wait_for_events(
        &mut self,
        once: bool,
        expectations: &[Expectation],
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + DEFAULT_TIMEOUT;
        let mut remaining: Vec<_> = expectations.iter().collect();
        while !remaining.is_empty() {
            self.recv_events().await?;
            remaining.retain(|ex| match ex {
                Expectation::Event { watch, kind, path } => {
                    let watch_id = match self.watches.get(watch) {
                        Some((id, _)) => *id,
                        None => return true,
                    };
                    let full_path = self.resolve_path(path);
                    match self.find_event(watch_id, *kind, &full_path) {
                        Some(idx) => {
                            self.pending_events.remove(idx);
                            false
                        }
                        None => true,
                    }
                }
                Expectation::Error { watch } => {
                    let watch_id = match self.watches.get(watch) {
                        Some((id, _)) => *id,
                        None => return true,
                    };
                    match self.find_error(watch_id) {
                        Some(idx) => {
                            self.pending_errors.remove(idx);
                            false
                        }
                        None => true,
                    }
                }
            });
            if once || remaining.is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timeout waiting for events: {:?}\n\
                     pending events: {:?}\n\
                     pending errors: {:?}",
                    remaining,
                    self.pending_events,
                    self.pending_errors
                );
            }
        }
        Ok(())
    }

    async fn wait_for_exactly(
        &mut self,
        required: &[Expectation],
        optional: &[Expectation],
    ) -> Result<()> {
        self.wait_for_events(false, required).await?;
        if !optional.is_empty() {
            let _ = self.wait_for_events(true, optional).await;
        }

        // Give a moment for any stragglers, then check for extras
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.recv_events().await?;

        if !self.pending_events.is_empty() || !self.pending_errors.is_empty() {
            bail!(
                "unexpected events remaining:\n\
                 pending events: {:?}\n\
                 pending errors: {:?}",
                self.pending_events,
                self.pending_errors
            );
        }
        Ok(())
    }

    async fn run_step(&mut self, step: &Step) -> Result<()> {
        match step {
            Watch { name, path, interest } => {
                let full_path = self.resolve_path(path);
                let watched = self.watcher.add(ArcPath::from(full_path), *interest)?;
                let id = watched.id();
                self.watches.insert(name, (id, watched));
            }
            Unwatch { name } => {
                if self.watches.remove(name).is_none() {
                    bail!("unknown watch: {name}");
                }
            }
            CreateFile { path } => {
                let full_path = self.resolve_path(path);
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                fs::write(&full_path, b"").await?;
            }
            CreateDir { path } => {
                let full_path = self.resolve_path(path);
                fs::create_dir_all(&full_path).await?;
            }
            WriteFile { path, content } => {
                let full_path = self.resolve_path(path);
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                fs::write(&full_path, content.as_bytes()).await?;
            }
            DeleteFile { path } => {
                let full_path = self.resolve_path(path);
                fs::remove_file(&full_path).await?;
            }
            DeleteDir { path } => {
                let full_path = self.resolve_path(path);
                fs::remove_dir_all(&full_path).await?;
            }
            Rename { from, to } => {
                let from_path = self.resolve_path(from);
                let to_path = self.resolve_path(to);
                if let Some(parent) = to_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                fs::rename(&from_path, &to_path).await?;
            }
            Sleep { ms } => {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
            }
            AtLeast(expectations) => {
                self.wait_for_events(false, expectations).await?;
            }
            Exactly(expectations) => self.wait_for_exactly(&expectations, &[]).await?,
            ExactlyWithOptional { required, optional } => {
                self.wait_for_exactly(required, optional).await?
            }
            Drain => {
                // Give events time to arrive, then clear them
                tokio::time::sleep(Duration::from_millis(200)).await;
                self.recv_events().await?;
                self.pending_events.clear();
                self.pending_errors.clear();
            }
        }
        Ok(())
    }
}

pub async fn run_test(steps: &[Step]) -> Result<()> {
    let _ = env_logger::try_init();
    let mut ctx = TestContext::new().await?;
    for (i, step) in steps.iter().enumerate() {
        ctx.run_step(step)
            .await
            .map_err(|e| anyhow::anyhow!("step {i} ({step:?}): {e}"))?;
    }
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn watch_existing_file() -> Result<()> {
    run_test(&[
        CreateFile { path: "foo.txt" },
        Watch {
            name: "w",
            path: "foo.txt",
            interest: Interest::Established | Interest::Modify,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "foo.txt",
        }]),
        WriteFile { path: "foo.txt", content: "hello" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::ModifyData,
            path: "foo.txt",
        }]),
    ])
    .await
}

#[tokio::test]
async fn watch_nested_nonexistent() -> Result<()> {
    run_test(&[
        Watch {
            name: "w",
            path: "a/b/c.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "a/b/c.txt",
        }]),
        // Create parent dirs and file
        CreateFile { path: "a/b/c.txt" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Create,
            path: "a/b/c.txt",
        }]),
    ])
    .await
}

#[tokio::test]
async fn multiple_watches_same_file() -> Result<()> {
    run_test(&[
        CreateFile { path: "shared.txt" },
        Watch {
            name: "w1",
            path: "shared.txt",
            interest: Interest::Established | Interest::Modify,
        },
        Watch {
            name: "w2",
            path: "shared.txt",
            interest: Interest::Established | Interest::Modify,
        },
        Exactly(&[
            Expectation::Event {
                watch: "w1",
                kind: Interest::Established,
                path: "shared.txt",
            },
            Expectation::Event {
                watch: "w2",
                kind: Interest::Established,
                path: "shared.txt",
            },
        ]),
        WriteFile { path: "shared.txt", content: "test" },
        ExactlyWithOptional {
            required: &[
                Expectation::Event {
                    watch: "w1",
                    kind: Interest::ModifyData,
                    path: "shared.txt",
                },
                Expectation::Event {
                    watch: "w2",
                    kind: Interest::ModifyData,
                    path: "shared.txt",
                },
            ],
            optional: &[
                Expectation::Event {
                    watch: "w1",
                    kind: Interest::ModifyData,
                    path: "shared.txt",
                },
                Expectation::Event {
                    watch: "w2",
                    kind: Interest::ModifyData,
                    path: "shared.txt",
                },
            ],
        },
    ])
    .await
}

#[tokio::test]
async fn unwatch() -> Result<()> {
    run_test(&[
        CreateFile { path: "file.txt" },
        Watch {
            name: "w",
            path: "file.txt",
            interest: Interest::Established | Interest::Modify,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "file.txt",
        }]),
        Unwatch { name: "w" },
        // give it time to take effect
        Sleep { ms: 200 },
        // Modify after unwatch - no event expected
        WriteFile { path: "file.txt", content: "ignored" },
        Exactly(&[]), // verify no events
    ])
    .await
}

#[tokio::test]
async fn nonexistent_create() -> Result<()> {
    run_test(&[
        // Watch a file that doesn't exist yet
        Watch {
            name: "w",
            path: "test.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "test.txt",
        }]),
        // Create the file - should get exactly one Create event
        CreateFile { path: "test.txt" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Create,
            path: "test.txt",
        }]),
    ])
    .await
}

#[tokio::test]
async fn delete() -> Result<()> {
    run_test(&[
        CreateFile { path: "test.txt" },
        Watch {
            name: "w",
            path: "test.txt",
            interest: Interest::Established | Interest::Delete,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "test.txt",
        }]),
        // Delete the file - Linux sends DeleteFile
        DeleteFile { path: "test.txt" },
        // Expect DeleteFile plus the error from failed re-watch attempt
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::DeleteFile,
            path: "test.txt",
        }]),
    ])
    .await
}

#[tokio::test]
async fn existing_file() -> Result<()> {
    run_test(&[
        CreateFile { path: "test.txt" },
        Watch {
            name: "w",
            path: "test.txt",
            interest: Interest::Established | Interest::Modify | Interest::Delete,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "test.txt",
        }]),
        WriteFile { path: "test.txt", content: "hello" },
        ExactlyWithOptional {
            required: &[Expectation::Event {
                watch: "w",
                kind: Interest::ModifyData,
                path: "test.txt",
            }],
            // sometimes on linux inotify produces an extra modify
            // event for a single fs::write_all.
            optional: &[Expectation::Event {
                watch: "w",
                kind: Interest::ModifyData,
                path: "test.txt",
            }],
        },
    ])
    .await
}

/// Watch a directory AND a non-existent file inside it.
/// Events should route to the correct watch.
#[tokio::test]
async fn watch_dir_and_nonexistent_child() -> Result<()> {
    run_test(&[
        CreateDir { path: "dir" },
        Watch {
            name: "dir",
            path: "dir",
            interest: Interest::Established | Interest::Create | Interest::Modify,
        },
        Watch {
            name: "file",
            path: "dir/file.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[
            Expectation::Event { watch: "dir", kind: Interest::Established, path: "dir" },
            Expectation::Event {
                watch: "file",
                kind: Interest::Established,
                path: "dir/file.txt",
            },
        ]),
        // Create the file - both watches will see it
        CreateFile { path: "dir/file.txt" },
        Exactly(&[
            Expectation::Event {
                watch: "dir",
                kind: Interest::CreateFile,
                path: "dir/file.txt",
            },
            Expectation::Event {
                watch: "file",
                kind: Interest::Create,
                path: "dir/file.txt",
            },
        ]),
    ])
    .await
}

/// Multiple watches on the same non-existent file.
/// Both should receive Create when it appears.
#[tokio::test]
async fn multiple_watches_nonexistent() -> Result<()> {
    run_test(&[
        Watch {
            name: "w1",
            path: "ghost.txt",
            interest: Interest::Established | Interest::Create,
        },
        Watch {
            name: "w2",
            path: "ghost.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[
            Expectation::Event {
                watch: "w1",
                kind: Interest::Established,
                path: "ghost.txt",
            },
            Expectation::Event {
                watch: "w2",
                kind: Interest::Established,
                path: "ghost.txt",
            },
        ]),
        CreateFile { path: "ghost.txt" },
        Exactly(&[
            Expectation::Event { watch: "w1", kind: Interest::Create, path: "ghost.txt" },
            Expectation::Event { watch: "w2", kind: Interest::Create, path: "ghost.txt" },
        ]),
    ])
    .await
}

/// File appears via rename, not create.
/// Watch should still detect it.
#[tokio::test]
async fn rename_to_watched_path() -> Result<()> {
    run_test(&[
        CreateFile { path: "source.txt" },
        Watch {
            name: "w",
            path: "target.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "target.txt",
        }]),
        Rename { from: "source.txt", to: "target.txt" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Create,
            path: "target.txt",
        }]),
    ])
    .await
}

/// Atomic file replacement pattern (common in editors):
/// write to temp file, rename over watched file.
#[tokio::test]
async fn atomic_file_replacement() -> Result<()> {
    run_test(&[
        CreateFile { path: "config.txt" },
        WriteFile { path: "config.txt", content: "v1" },
        Watch {
            name: "w",
            path: "config.txt",
            interest: Interest::Established | Interest::Modify | Interest::Delete,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "config.txt",
        }]),
        // Atomic replacement: write temp, rename over original
        WriteFile { path: "config.txt.tmp", content: "v2" },
        Rename { from: "config.txt.tmp", to: "config.txt" },
        // Linux sees the old file deleted (DeleteFile) when renamed over
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::DeleteFile,
            path: "config.txt",
        }]),
    ])
    .await
}

/// Delete parent directory, not the watched file directly.
#[tokio::test]
async fn delete_parent_directory() -> Result<()> {
    run_test(&[
        CreateFile { path: "parent/child.txt" },
        Watch {
            name: "w",
            path: "parent/child.txt",
            interest: Interest::Established | Interest::Delete,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "parent/child.txt",
        }]),
        // Delete the parent, which implicitly deletes the child
        DeleteDir { path: "parent" },
        // Should get a delete event for the watched file
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::DeleteFile,
            path: "parent/child.txt",
        }]),
    ])
    .await
}

/// Rapid create/delete cycles - test for race conditions.
#[tokio::test]
async fn rapid_create_delete_cycles() -> Result<()> {
    run_test(&[
        Watch {
            name: "w",
            path: "ephemeral.txt",
            interest: Interest::Established | Interest::Create | Interest::Delete,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "ephemeral.txt",
        }]),
        // Rapid cycles
        CreateFile { path: "ephemeral.txt" },
        DeleteFile { path: "ephemeral.txt" },
        CreateFile { path: "ephemeral.txt" },
        DeleteFile { path: "ephemeral.txt" },
        Exactly(&[
            Expectation::Event {
                watch: "w",
                kind: Interest::Create,
                path: "ephemeral.txt",
            },
            Expectation::Event {
                watch: "w",
                kind: Interest::Create,
                path: "ephemeral.txt",
            },
            Expectation::Event {
                watch: "w",
                kind: Interest::Delete,
                path: "ephemeral.txt",
            },
            Expectation::Event {
                watch: "w",
                kind: Interest::Delete,
                path: "ephemeral.txt",
            },
        ]),
    ])
    .await
}

/// Watch deeply nested non-existent path, create intermediate dirs one at a time.
#[tokio::test]
async fn create_intermediate_dirs_slowly() -> Result<()> {
    run_test(&[
        Watch {
            name: "w",
            path: "a/b/c/d/e.txt",
            interest: Interest::Established | Interest::Create,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "a/b/c/d/e.txt",
        }]),
        // Create directories one at a time
        CreateDir { path: "a" },
        Sleep { ms: 100 },
        CreateDir { path: "a/b" },
        Sleep { ms: 100 },
        CreateDir { path: "a/b/c" },
        Sleep { ms: 100 },
        CreateDir { path: "a/b/c/d" },
        Sleep { ms: 100 },
        // Finally create the file
        CreateFile { path: "a/b/c/d/e.txt" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Create,
            path: "a/b/c/d/e.txt",
        }]),
    ])
    .await
}

/// Rename a parent directory far up the chain.
/// Watch on /a/b/c/d.txt, rename /a/b to /a/x
#[tokio::test]
async fn rename_parent_directory() -> Result<()> {
    run_test(&[
        CreateFile { path: "a/b/c/d.txt" },
        Watch {
            name: "w",
            path: "a/b/c/d.txt",
            interest: Interest::Established | Interest::Delete | Interest::Modify,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "a/b/c/d.txt",
        }]),
        // Rename parent - the watched path no longer exists!
        Rename { from: "a/b", to: "a/x" },
        // Should see delete since the path is gone
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Delete,
            path: "a/b/c/d.txt",
        }]),
    ])
    .await
}

/// Replace a file with a directory of the same name.
#[tokio::test]
async fn replace_file_with_directory() -> Result<()> {
    run_test(&[
        CreateFile { path: "thing" },
        Watch {
            name: "w",
            path: "thing",
            interest: Interest::Established | Interest::Delete | Interest::Create,
        },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Established,
            path: "thing",
        }]),
        DeleteFile { path: "thing" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::DeleteFile,
            path: "thing",
        }]),
        // Now create a directory with the same name
        CreateDir { path: "thing" },
        Exactly(&[Expectation::Event {
            watch: "w",
            kind: Interest::Create,
            path: "thing",
        }]),
    ])
    .await
}
