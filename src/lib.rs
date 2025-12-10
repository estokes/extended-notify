//! Extended filesystem watcher built on [notify].
//!
//! This crate extends notify with several features:
//!
//! - **Watch non-existent paths**: When you watch a path that doesn't exist,
//!   the nearest existing ancestor is watched instead, and polling detects
//!   when the target path comes into existence. This is useful for watching
//!   config files that may be created later, or paths on removable media.
//!
//! - **Interest-based filtering**: Rather than receiving all events and
//!   filtering yourself, specify exactly which event types matter to you.
//!   Interests are hierarchical—subscribing to [`Interest::Delete`] receives
//!   all deletion events, while [`Interest::DeleteFile`] only receives file
//!   deletions.
//!
//! - **RAII watch handles**: The [`Watched`] handle returned by [`Watcher::add`]
//!   automatically stops the watch when dropped. No manual cleanup required.
//!
//! - **Synthetic events**: When a non-existent path comes into existence (or
//!   vice versa), synthetic [`Interest::Create`] and [`Interest::Delete`]
//!   events are generated if you've subscribed to them.
//!
//! - **Establishment notification**: Subscribe to [`Interest::Established`] to
//!   receive a synthetic event when the watch is first set up, useful for
//!   triggering an initial read of the watched path.
//!
//! - **Multiple Watches**: Multiple watches for the same file or
//!   directory are handled gracefully. Both watches will behave
//!   correctly, but only one `Notify` watch will be created.
//!
//! # Example
//!
//! ```no_run
//! use extended_notify::{ArcPath, EventBatch, EventHandler, Interest, WatcherConfigBuilder};
//! use anyhow::Result;
//! use enumflags2::make_bitflags;
//!
//! #[derive(Clone)]
//! struct MyHandler;
//!
//! impl EventHandler for MyHandler {
//!     async fn handle_event(&mut self, batch: EventBatch) -> Result<()> {
//!         for (id, event) in batch.iter() {
//!             println!("{:?}: {:?}", id, event);
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # async fn example() -> Result<()> {
//! let watcher = WatcherConfigBuilder::default()
//!     .event_handler(MyHandler)
//!     .build()?
//!     .start()?;
//!
//! // Watch a path that may or may not exist yet
//! let interest = make_bitflags!(Interest::{Create | Delete | Modify});
//! let handle = watcher.add(ArcPath::from("/tmp/my-config"), interest)?;
//!
//! // Events flow to MyHandler::handle_event
//! // Watch stops when handle is dropped
//! # Ok(())
//! # }
//! ```
//!
//! # Event Delivery
//!
//! Events are delivered in batches through the [`EventHandler`] trait. Each
//! batch contains `(Id, Event)` pairs, where the [`Id`] identifies which watch
//! generated the event. Multiple events for the same watch may be coalesced
//! into a single [`Event`] with multiple paths.
//!
//! If your handler returns an error, the watcher task stops and all subsequent
//! operations on the [`Watcher`] will fail.
//!
//! # Polling
//!
//! Polling runs alongside native filesystem notifications to handle cases
//! notifications can't cover—primarily detecting when non-existent paths come
//! into existence. By default, 100 paths are checked each second. Adjust with
//! [`Watcher::set_poll_interval`] and [`Watcher::set_poll_batch`], or disable
//! polling entirely by setting the batch size to 0.
//!
//! If you disable polling creation of a file and multiple levels of
//! directories may not be detected. For example if you watch a file
//! /foo/bar/baz that does not exist, and you disable polling, if only
//! /foo exists to start, and /foo/bar is created, then /foo/bar/baz
//! is created, you may not get the create event for /foo/bar/baz.

use anyhow::{bail, Result};
use derive_builder::Builder;
use enumflags2::{bitflags, BitFlags};
use fxhash::FxHashSet;
use notify::event::{
    AccessKind, CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind, RenameMode,
};
use notify_debouncer_full::{DebounceEventHandler, DebounceEventResult};
use poolshark::global::GPooled;
use std::{
    borrow::Borrow,
    hash::Hash,
    ops::Deref,
    path::{self, Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{sync::mpsc, task};
use watch_task::MAX_NOTIFY_BATCH;

mod watch_task;

#[cfg(test)]
mod test;

pub const MIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u64);

impl Id {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArcPath(Arc<PathBuf>);

impl ArcPath {
    pub fn get_mut(&mut self) -> Option<&mut PathBuf> {
        Arc::get_mut(&mut self.0)
    }

    pub fn make_mut(&mut self) -> &mut PathBuf {
        Arc::make_mut(&mut self.0)
    }

    pub fn root() -> Self {
        static ROOT: LazyLock<ArcPath> =
            LazyLock::new(|| ArcPath::from(path::MAIN_SEPARATOR_STR));
        ROOT.clone()
    }
}

impl AsRef<Path> for ArcPath {
    fn as_ref(&self) -> &Path {
        &*self.0
    }
}

impl Deref for ArcPath {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl Borrow<Path> for ArcPath {
    fn borrow(&self) -> &Path {
        &*self.0
    }
}

impl From<&Path> for ArcPath {
    fn from(value: &Path) -> Self {
        Self(Arc::new(value.into()))
    }
}

impl From<&str> for ArcPath {
    fn from(value: &str) -> Self {
        Self(Arc::new(PathBuf::from(value)))
    }
}

impl From<PathBuf> for ArcPath {
    fn from(value: PathBuf) -> Self {
        Self(Arc::new(value))
    }
}

impl From<&PathBuf> for ArcPath {
    fn from(value: &PathBuf) -> Self {
        Self(Arc::new(value.clone()))
    }
}

impl From<&ArcPath> for ArcPath {
    fn from(value: &ArcPath) -> Self {
        value.clone()
    }
}

impl PartialEq<Path> for ArcPath {
    fn eq(&self, other: &Path) -> bool {
        &*self.0 == other
    }
}

impl PartialOrd<Path> for ArcPath {
    fn partial_cmp(&self, other: &Path) -> Option<std::cmp::Ordering> {
        (**self.0).partial_cmp(other)
    }
}

impl PartialEq<PathBuf> for ArcPath {
    fn eq(&self, other: &PathBuf) -> bool {
        &*self.0 == other
    }
}

impl PartialOrd<PathBuf> for ArcPath {
    fn partial_cmp(&self, other: &PathBuf) -> Option<std::cmp::Ordering> {
        (**self.0).partial_cmp(other)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[bitflags]
#[repr(u64)]
pub enum Interest {
    /// Synthetic event fired when a watch is first established.
    ///
    /// This is not a filesystem event—it's generated immediately when the
    /// watch is set up, regardless of whether the path exists. Useful for
    /// triggering an initial read of watched files.
    Established,
    /// Matches any event type.
    Any,
    Access,
    AccessOpen,
    AccessClose,
    AccessRead,
    AccessOther,
    Create,
    CreateFile,
    CreateFolder,
    CreateOther,
    Modify,
    ModifyData,
    ModifyDataSize,
    ModifyDataContent,
    ModifyDataOther,
    ModifyMetadata,
    ModifyMetadataAccessTime,
    ModifyMetadataWriteTime,
    ModifyMetadataPermissions,
    ModifyMetadataOwnership,
    ModifyMetadataExtended,
    ModifyMetadataOther,
    ModifyRename,
    ModifyRenameTo,
    ModifyRenameFrom,
    ModifyRenameBoth,
    ModifyRenameOther,
    ModifyOther,
    Delete,
    DeleteFile,
    DeleteFolder,
    DeleteOther,
    Other,
}

impl From<&notify::EventKind> for Interest {
    fn from(kind: &notify::EventKind) -> Self {
        match kind {
            notify::EventKind::Any => Self::Any,
            notify::EventKind::Access(AccessKind::Any) => Self::Access,
            notify::EventKind::Access(AccessKind::Close(_)) => Self::AccessClose,
            notify::EventKind::Access(AccessKind::Open(_)) => Self::AccessOpen,
            notify::EventKind::Access(AccessKind::Read) => Self::AccessRead,
            notify::EventKind::Access(AccessKind::Other) => Self::AccessOther,
            notify::EventKind::Create(CreateKind::Any) => Self::Create,
            notify::EventKind::Create(CreateKind::File) => Self::CreateFile,
            notify::EventKind::Create(CreateKind::Folder) => Self::CreateFolder,
            notify::EventKind::Create(CreateKind::Other) => Self::CreateOther,
            notify::EventKind::Modify(ModifyKind::Any) => Self::Modify,
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Any)) => {
                Self::ModifyData
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Content)) => {
                Self::ModifyDataContent
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Size)) => {
                Self::ModifyDataSize
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Other)) => {
                Self::ModifyDataOther
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)) => {
                Self::ModifyMetadata
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime)) => {
                Self::ModifyMetadataAccessTime
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Extended)) => {
                Self::ModifyMetadataExtended
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Other)) => {
                Self::ModifyMetadataOther
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Ownership)) => {
                Self::ModifyMetadataOwnership
            }
            notify::EventKind::Modify(ModifyKind::Metadata(
                MetadataKind::Permissions,
            )) => Self::ModifyMetadataPermissions,
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::WriteTime)) => {
                Self::ModifyMetadataWriteTime
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Any)) => {
                Self::ModifyRename
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                Self::ModifyRenameBoth
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                Self::ModifyRenameFrom
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                Self::ModifyRenameTo
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Other)) => {
                Self::ModifyRenameOther
            }
            notify::EventKind::Modify(ModifyKind::Other) => Self::ModifyOther,
            notify::EventKind::Remove(RemoveKind::Any) => Self::Delete,
            notify::EventKind::Remove(RemoveKind::File) => Self::DeleteFile,
            notify::EventKind::Remove(RemoveKind::Folder) => Self::DeleteFolder,
            notify::EventKind::Remove(RemoveKind::Other) => Self::DeleteOther,
            notify::EventKind::Other => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
struct Watch {
    path: ArcPath,
    id: Id,
    interest: BitFlags<Interest>,
}

impl Watch {
    fn interested(&self, kind: &notify::EventKind) -> bool {
        use Interest::*;
        if self.interest.contains(Any) {
            return true;
        }
        match kind {
            notify::EventKind::Any => !self.interest.is_empty(),
            notify::EventKind::Access(AccessKind::Any) => self
                .interest
                .intersects(Access | AccessClose | AccessOpen | AccessRead | AccessOther),
            notify::EventKind::Access(AccessKind::Close(_)) => {
                self.interest.intersects(Access | AccessClose)
            }
            notify::EventKind::Access(AccessKind::Open(_)) => {
                self.interest.intersects(Access | AccessOpen)
            }
            notify::EventKind::Access(AccessKind::Read) => {
                self.interest.intersects(Access | AccessRead)
            }
            notify::EventKind::Access(AccessKind::Other) => {
                self.interest.intersects(Access | AccessOther)
            }
            notify::EventKind::Create(CreateKind::Any) => {
                self.interest.intersects(Create | CreateFile | CreateFolder | CreateOther)
            }
            notify::EventKind::Create(CreateKind::File) => {
                self.interest.intersects(Create | CreateFile)
            }
            notify::EventKind::Create(CreateKind::Folder) => {
                self.interest.intersects(Create | CreateFolder)
            }
            notify::EventKind::Create(CreateKind::Other) => {
                self.interest.intersects(Create | CreateOther)
            }
            notify::EventKind::Modify(ModifyKind::Any) => self.interest.intersects(
                Modify
                    | ModifyData
                    | ModifyDataSize
                    | ModifyDataContent
                    | ModifyDataOther
                    | ModifyMetadata
                    | ModifyMetadataAccessTime
                    | ModifyMetadataWriteTime
                    | ModifyMetadataPermissions
                    | ModifyMetadataOwnership
                    | ModifyMetadataExtended
                    | ModifyMetadataOther
                    | ModifyRename
                    | ModifyRenameTo
                    | ModifyRenameFrom
                    | ModifyRenameBoth
                    | ModifyRenameOther
                    | ModifyOther,
            ),
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Any)) => {
                self.interest.intersects(
                    Modify
                        | ModifyData
                        | ModifyDataSize
                        | ModifyDataContent
                        | ModifyDataOther,
                )
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Content)) => {
                self.interest.intersects(Modify | ModifyData | ModifyDataContent)
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Size)) => {
                self.interest.intersects(Modify | ModifyData | ModifyDataSize)
            }
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Other)) => {
                self.interest.intersects(Modify | ModifyData | ModifyDataOther)
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)) => {
                self.interest.intersects(
                    Modify
                        | ModifyMetadata
                        | ModifyMetadataAccessTime
                        | ModifyMetadataWriteTime
                        | ModifyMetadataPermissions
                        | ModifyMetadataOwnership
                        | ModifyMetadataExtended
                        | ModifyMetadataOther,
                )
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime)) => {
                self.interest
                    .intersects(Modify | ModifyMetadata | ModifyMetadataAccessTime)
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Extended)) => {
                self.interest.intersects(Modify | ModifyMetadata | ModifyMetadataExtended)
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Other)) => {
                self.interest.intersects(Modify | ModifyMetadata | ModifyMetadataOther)
            }
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::Ownership)) => {
                self.interest
                    .intersects(Modify | ModifyMetadata | ModifyMetadataOwnership)
            }
            notify::EventKind::Modify(ModifyKind::Metadata(
                MetadataKind::Permissions,
            )) => self
                .interest
                .intersects(Modify | ModifyMetadata | ModifyMetadataPermissions),
            notify::EventKind::Modify(ModifyKind::Metadata(MetadataKind::WriteTime)) => {
                self.interest
                    .intersects(Modify | ModifyMetadata | ModifyMetadataWriteTime)
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Any)) => {
                self.interest.intersects(
                    Modify
                        | ModifyRename
                        | ModifyRenameTo
                        | ModifyRenameFrom
                        | ModifyRenameBoth
                        | ModifyRenameOther,
                )
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                self.interest.intersects(Modify | ModifyRename | ModifyRenameBoth)
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                self.interest.intersects(Modify | ModifyRename | ModifyRenameFrom)
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                self.interest.intersects(Modify | ModifyRename | ModifyRenameTo)
            }
            notify::EventKind::Modify(ModifyKind::Name(RenameMode::Other)) => {
                self.interest.intersects(Modify | ModifyRename | ModifyRenameOther)
            }
            notify::EventKind::Modify(ModifyKind::Other) => {
                self.interest.intersects(Modify | ModifyOther)
            }
            notify::EventKind::Remove(RemoveKind::Any) => {
                self.interest.intersects(Delete | DeleteFile | DeleteFolder | DeleteOther)
            }
            notify::EventKind::Remove(RemoveKind::File) => {
                self.interest.intersects(Delete | DeleteFile)
            }
            notify::EventKind::Remove(RemoveKind::Folder) => {
                self.interest.intersects(Delete | DeleteFolder)
            }
            notify::EventKind::Remove(RemoveKind::Other) => {
                self.interest.intersects(Delete | DeleteOther)
            }
            notify::EventKind::Other => self.interest.contains(Other),
        }
    }
}

#[derive(Debug)]
enum Cmd {
    Watch(Watch),
    Stop(Id),
    SetPollInterval(Duration),
    SetPollBatch(usize),
}

struct NotifyChan(mpsc::Sender<DebounceEventResult>);

impl DebounceEventHandler for NotifyChan {
    fn handle_event(&mut self, event: notify_debouncer_full::DebounceEventResult) {
        let _ = self.0.blocking_send(event);
    }
}

#[derive(Debug, Clone)]
pub enum EventKind {
    Error(Arc<anyhow::Error>),
    Event(Interest),
}

#[derive(Debug)]
pub struct Event {
    pub paths: GPooled<FxHashSet<ArcPath>>,
    pub event: EventKind,
}

/// A batch of events delivered to the [`EventHandler`].
pub type EventBatch = GPooled<Vec<(Id, Event)>>;

pub trait EventHandler: Send + 'static {
    fn handle_event(
        &mut self,
        event: EventBatch,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl EventHandler for mpsc::Sender<EventBatch> {
    fn handle_event(
        &mut self,
        event: EventBatch,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(self.send(event).await?) }
    }
}

impl EventHandler for futures::channel::mpsc::Sender<EventBatch> {
    fn handle_event(
        &mut self,
        event: EventBatch,
    ) -> impl Future<Output = Result<()>> + Send {
        use futures::SinkExt;
        async { Ok(self.send(event).await?) }
    }
}

/// A watched path
///
/// The watch will be stopped when this object is dropped.  This
/// object is essentially the `Id`, you can use this as a key in a
/// `HashMap` and look up the entry by `Id`
#[derive(Debug)]
pub struct Watched {
    id: Id,
    watcher: Watcher,
}

impl Watched {
    /// Get the `Id` of this watched
    pub fn id(&self) -> Id {
        self.id
    }
}

impl Borrow<Id> for Watched {
    fn borrow(&self) -> &Id {
        &self.id
    }
}

impl<'a> Borrow<Id> for &'a Watched {
    fn borrow(&self) -> &'a Id {
        &self.id
    }
}

impl PartialEq for Watched {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Watched {}

impl PartialOrd for Watched {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl Ord for Watched {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for Watched {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        let _ = self.watcher.0.send(Cmd::Stop(self.id));
    }
}

#[derive(Debug, Clone, Builder)]
pub struct WatcherConfig<T: EventHandler> {
    /// The debounce timeout, events will not arrive faster than this. Default 250ms.
    #[builder(default = "Duration::from_millis(250)")]
    timeout: Duration,
    /// How often the debouncer ticks. Default 1/4 of the timeout
    #[builder(setter(strip_option), default)]
    tick_rate: Option<Duration>,
    /// The poll interval determines how often to poll a batch of
    /// files. Polling is necessary to cover cases that filesystem
    /// notifications can't handle, such as watching a path that
    /// doesn't yet exist. Each poll interval, poll batch files will
    /// be checked in parallel.
    ///
    /// The minimum poll interval is 100ms, an error will be returned
    /// if you try to set the value lower than that.
    ///
    /// default 1 second
    #[builder(default = "Duration::from_secs(1)")]
    poll_interval: Duration,

    /// How many files to poll each poll interval. If this is set to 0
    /// polling is disabled.
    ///
    /// default 100
    #[builder(default = "100")]
    poll_batch: usize,
    /// Where to send the events (required)
    event_handler: T,
}

impl<T: EventHandler> WatcherConfig<T> {
    /// Start the watcher
    pub fn start(self) -> Result<Watcher> {
        let (notify_tx, notify_rx) = mpsc::channel(MAX_NOTIFY_BATCH);
        let watcher = notify_debouncer_full::new_debouncer(
            self.timeout,
            self.tick_rate,
            NotifyChan(notify_tx),
        )?;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        task::spawn(watch_task::watcher_loop(
            self.poll_interval,
            self.poll_batch,
            watcher,
            notify_rx,
            cmd_rx,
            self.event_handler,
        ));
        Ok(Watcher(cmd_tx))
    }
}

/// An filesystem watcher
///
/// When this object is dropped the background task associated with it
/// will stop. You may still receive some events on your provided
/// event handler even after this, you can safely ignore them, or drop
/// the handler.
///
/// If your provided event handler returns an error, the background
/// task associated with this object will stop. From then on all the
/// methods of this object will fail.
#[derive(Debug, Clone)]
pub struct Watcher(mpsc::UnboundedSender<Cmd>);

impl Watcher {
    /// Add a new watch
    ///
    /// Fails if the watcher task has died. Other errors will be sent
    /// as events.
    ///
    /// The watch ends when the returned `Watched` is dropped. However
    /// it cannot be guaranteed that no events will happen for a watch
    /// after it has been dropped. You can handle this by ignoring
    /// events with an unknown `Id`.
    pub fn add(&self, path: ArcPath, interest: BitFlags<Interest>) -> Result<Watched> {
        let id = Id::new();
        self.0.send(Cmd::Watch(Watch { path, interest, id }))?;
        Ok(Watched { id, watcher: self.clone() })
    }

    /// Set the poll interval
    ///
    /// The poll interval determines how often to poll a batch of
    /// files. Polling is necessary to cover cases that filesystem
    /// notifications can't handle, such as watching a path that
    /// doesn't yet exist. Each poll interval, poll batch files will
    /// be checked in parallel.
    ///
    /// The minimum poll interval is 100ms, an error will be returned
    /// if you try to set the value lower than that.
    pub fn set_poll_interval(&self, t: Duration) -> Result<()> {
        if t < MIN_POLL_INTERVAL {
            bail!("poll interval may not be less than {MIN_POLL_INTERVAL:?}")
        }
        Ok(self.0.send(Cmd::SetPollInterval(t))?)
    }

    /// Set the size of a poll batch
    ///
    /// How many files to poll each poll interval. If this is set to 0
    /// polling is disabled.
    pub fn set_poll_batch(&self, n: usize) -> Result<()> {
        Ok(self.0.send(Cmd::SetPollBatch(n))?)
    }
}
