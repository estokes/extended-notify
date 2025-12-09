use crate::{
    ArcPath, Cmd, Event, EventBatch, EventHandler, EventKind, Id, Interest, Watch,
};
use anyhow::anyhow;
use futures::future::join_all;
use fxhash::{FxHashMap, FxHashSet};
use notify::{
    event::{ModifyKind, RenameMode},
    RecommendedWatcher, RecursiveMode, Watcher,
};
use poolshark::{
    global::{GPooled, Pool},
    local::LPooled,
};
use std::{
    collections::{hash_set, VecDeque},
    ffi::OsString,
    path::Path,
    result::Result,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{fs, select, sync::mpsc};

static PATHS: LazyLock<Pool<FxHashSet<ArcPath>>> =
    LazyLock::new(|| Pool::new(1000, 1000));
static BATCH_POOL: LazyLock<Pool<Vec<(Id, Event)>>> =
    LazyLock::new(|| Pool::new(1024, 1024));

pub(super) const MAX_NOTIFY_BATCH: usize = 10_000;
pub(super) const MAX_CMD_BATCH: usize = 10_000;
const POLL_BATCH: usize = 100;
const POLL_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathStatus {
    // the canonical part of the path that exists
    exists: ArcPath,
    // the path parts that are missing, in reverse order
    missing: LPooled<Vec<OsString>>,
    // the full path including the missing parts, which aren't canonical
    full_path: ArcPath,
}

impl PathStatus {
    fn exists(&self) -> bool {
        self.missing.is_empty()
    }

    async fn new(path: &Path) -> Self {
        let mut missing: LPooled<Vec<OsString>> = LPooled::take();
        let mut root: &Path = &path;
        let mut t = loop {
            match fs::canonicalize(root).await {
                Ok(exists) => {
                    let exists = ArcPath::from(exists);
                    break Self { exists: exists.clone(), missing, full_path: exists };
                }
                Err(_) => match (root.parent(), root.file_name()) {
                    (None, None) => {
                        break Self {
                            exists: ArcPath::root(),
                            missing,
                            full_path: ArcPath::from(path),
                        };
                    }
                    (None, Some(_)) => {
                        break Self {
                            exists: ArcPath::root(),
                            missing,
                            full_path: ArcPath::from(path),
                        };
                    }
                    (Some(parent), None) => root = parent,
                    (Some(parent), Some(file)) => {
                        missing.push(OsString::from(file));
                        root = parent;
                    }
                },
            }
        };
        if !t.missing.is_empty() {
            let full_path = t.full_path.make_mut();
            for part in t.missing.iter().rev() {
                full_path.push(part);
            }
        }
        t
    }
}

#[derive(Debug, Clone)]
struct WatchInt {
    watch: Watch,
    path_status: PathStatus,
}

#[derive(Debug)]
enum AddAction {
    AddWatch { path: ArcPath, notify: bool },
    AddPending { watch_path: ArcPath, full_path: ArcPath, notify: bool },
}

#[derive(Debug)]
struct ChangeOfStatus {
    remove: Option<ArcPath>,
    add: Option<AddAction>,
    syn: bool,
}

#[derive(Default)]
struct Watched {
    by_id: FxHashMap<Id, WatchInt>,
    by_root: FxHashMap<ArcPath, FxHashSet<Id>>,
    to_poll: Vec<Id>,
    poll_batch: Option<usize>,
}

impl Watched {
    /// add a watch and return the necessary action to the notify::Watcher
    async fn add_watch(&mut self, w: Watch) -> AddAction {
        let id = w.id;
        let path_status = PathStatus::new(&Path::new(&*w.path)).await;
        let w = WatchInt { watch: w, path_status };
        let watch_path = w.path_status.exists.clone();
        let full_path = w.path_status.full_path.clone();
        let exists = w.path_status.exists();
        let notify = w.watch.interest.contains(Interest::Established);
        self.by_root.entry(watch_path.clone()).or_default().insert(id);
        self.by_id.insert(id, w);
        if exists {
            AddAction::AddWatch { path: watch_path, notify }
        } else {
            AddAction::AddPending { watch_path, full_path, notify }
        }
    }

    /// remove a watch, and return an optional action to be performed on the
    /// watcher
    fn remove_watch(&mut self, id: &Id) -> (Option<WatchInt>, Option<ArcPath>) {
        let w = self.by_id.remove(id);
        let to_stop =
            w.as_ref().and_then(|w| match self.by_root.get_mut(&w.path_status.exists) {
                None => Some(w.path_status.exists.clone()),
                Some(ids) => {
                    ids.remove(id);
                    ids.is_empty().then(|| {
                        self.by_root.remove(&w.path_status.exists);
                        w.path_status.exists.clone()
                    })
                }
            });
        (w, to_stop)
    }

    /// inform of a change of status for this watch id, return a set of
    /// necessary actions to the notify::Watcher
    async fn change_status(&mut self, id: Id) -> ChangeOfStatus {
        let (w, remove) = self.remove_watch(&id);
        // syn true means we need to generate a synthetic delete
        // for a delete that otherwise would not be reported
        let mut syn = false;
        let add = match w {
            Some(w) => {
                let did_exist = w.path_status.exists();
                let old_exists = w.path_status.exists.clone();
                let action = self.add_watch(w.watch).await;
                // syn = true only when transitioning from file to parent (file deleted)
                // i.e., old_exists is a descendant of new_exists
                if let Some(new_w) = self.by_id.get(&id) {
                    syn = did_exist
                        && old_exists.starts_with(&*new_w.path_status.exists)
                        && old_exists != new_w.path_status.exists;
                }
                Some(action)
            }
            None => None,
        };
        let (remove, add) = match (&remove, &add) {
            (Some(remove), Some(AddAction::AddPending { watch_path, .. }))
            | (Some(remove), Some(AddAction::AddWatch { path: watch_path, .. }))
                if remove == watch_path =>
            {
                (None, None)
            }
            (Some(_), Some(AddAction::AddPending { .. }))
            | (Some(_), Some(AddAction::AddWatch { .. }))
            | (None, Some(_))
            | (Some(_), None)
            | (None, None) => (remove, add),
        };
        ChangeOfStatus { remove, add, syn }
    }

    fn relevant_to<'a>(&'a self, path: &'a Path) -> impl Iterator<Item = &'a WatchInt> {
        struct I<'a> {
            level: usize,
            ids: LPooled<VecDeque<hash_set::Iter<'a, Id>>>,
            t: &'a Watched,
        }
        impl<'a> Iterator for I<'a> {
            type Item = &'a WatchInt;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    match self.ids.front_mut() {
                        None => break None,
                        Some(set) => match set.next() {
                            Some(id) => match self.t.by_id.get(id) {
                                Some(w)
                                // limit established paths to the path itself and it's immediate parent
                                if self.level < 2 || !w.path_status.exists() =>
                                {
                                    break Some(w)
                                }
                                Some(_) | None => continue,
                            },
                            None => {
                                self.level += 1;
                                self.ids.pop_front();
                            }
                        },
                    }
                }
            }
        }
        let mut ids: LPooled<VecDeque<hash_set::Iter<'a, Id>>> = LPooled::take();
        let mut root = Some(path);
        while let Some(path) = root {
            if let Some(h) = self.by_root.get(path) {
                ids.push_back(h.iter())
            }
            root = path.parent();
        }
        I { level: 0, ids, t: self }
    }

    fn poll_batch(&self) -> usize {
        self.poll_batch.unwrap_or(POLL_BATCH)
    }

    /// poll all watches and return a list of ids who's status might have changed
    async fn poll_cycle(&mut self) -> LPooled<Vec<Id>> {
        if self.to_poll.is_empty() {
            self.to_poll.extend(self.by_id.keys().map(|id| *id));
        }
        let poll_batch = self.poll_batch();
        let mut to_check: LPooled<Vec<&WatchInt>> = LPooled::take();
        let mut i = 0;
        while i < poll_batch
            && let Some(id) = self.to_poll.pop()
        {
            i += 1;
            if let Some(w) = self.by_id.get(&id) {
                to_check.push(w)
            }
        }
        join_all(to_check.drain(..).map(|w| async {
            let exists =
                tokio::time::timeout(POLL_TIMEOUT, tokio::fs::try_exists(&*w.watch.path))
                    .await
                    .unwrap_or(Ok(false))
                    .unwrap_or(false);
            let established = w.path_status.exists();
            if (established && !exists) || (!established && exists) {
                Some(w.watch.id)
            } else {
                None
            }
        }))
        .await
        .into_iter()
        .filter_map(|x| x)
        .collect()
    }

    async fn process_event(
        &mut self,
        batch: &mut GPooled<Vec<(Id, Event)>>,
        ev: Result<notify::Event, notify::Error>,
    ) -> LPooled<Vec<Id>> {
        let mut by_id: LPooled<FxHashMap<Id, Event>> = LPooled::take();
        let mut status_changed: LPooled<Vec<Id>> = LPooled::take();
        log::debug!("processing notify event {ev:?}");
        match ev {
            Ok(ev) => {
                let event = EventKind::Event((&ev.kind).into());
                for path in ev.paths {
                    let path = ArcPath::from(path);
                    for w in self.relevant_to(&*path) {
                        if w.path_status.exists() {
                            if w.watch.interested(&ev.kind) {
                                let wev = by_id.entry(w.watch.id).or_insert_with(|| {
                                    Event { event: event.clone(), paths: PATHS.take() }
                                });
                                wev.paths.insert(path.clone());
                            }
                            match &ev.kind {
                                notify::EventKind::Remove(_)
                                | notify::EventKind::Modify(ModifyKind::Name(
                                    RenameMode::From,
                                )) if &w.path_status.exists == &*path => {
                                    status_changed.push(w.watch.id)
                                }
                                notify::EventKind::Any
                                | notify::EventKind::Access(_)
                                | notify::EventKind::Create(_)
                                | notify::EventKind::Modify(_)
                                | notify::EventKind::Remove(_)
                                | notify::EventKind::Other => (),
                            }
                        } else {
                            match &ev.kind {
                                notify::EventKind::Create(_)
                                | notify::EventKind::Modify(ModifyKind::Name(
                                    RenameMode::To,
                                )) => {
                                    status_changed.push(w.watch.id);
                                }
                                notify::EventKind::Any
                                | notify::EventKind::Access(_)
                                | notify::EventKind::Modify(_)
                                | notify::EventKind::Remove(_)
                                | notify::EventKind::Other => (),
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut paths: LPooled<Vec<_>> =
                    e.paths.iter().map(|b| ArcPath::from(b)).collect();
                let is_not_found = matches!(
                    &e.kind,
                    notify::ErrorKind::PathNotFound | notify::ErrorKind::WatchNotFound
                );
                let err = Arc::new(anyhow!(e));
                for path in paths.drain(..) {
                    for w in self.relevant_to(&path) {
                        if is_not_found {
                            status_changed.push(w.watch.id)
                        } else {
                            let wev = by_id.entry(w.watch.id).or_insert_with(|| Event {
                                paths: PATHS.take(),
                                event: EventKind::Error(err.clone()),
                            });
                            wev.paths.insert(path.clone());
                        }
                    }
                }
            }
        }
        batch.extend(by_id.drain().map(|(id, wev)| (id, wev)));
        status_changed
    }
}

fn push_error(batch: &mut EventBatch, id: Id, path: Option<ArcPath>, e: anyhow::Error) {
    let mut wev = Event { paths: PATHS.take(), event: EventKind::Error(Arc::new(e)) };
    if let Some(path) = path {
        wev.paths.insert(path);
    }
    batch.push((id, wev))
}

fn push_event(batch: &mut EventBatch, id: Id, path: ArcPath, event: EventKind) {
    let mut wev = Event { paths: PATHS.take(), event };
    wev.paths.insert(path);
    batch.push((id, wev))
}

pub(super) async fn watcher_loop<T: EventHandler>(
    mut watcher: RecommendedWatcher,
    mut rx_notify: mpsc::Receiver<notify::Result<notify::Event>>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    mut tx: T,
) {
    let mut watched = Watched::default();
    let mut recv_buf = vec![];
    let mut cmd_buf = vec![];
    let mut batch = BATCH_POOL.take();
    let mut poll_interval = tokio::time::interval(DEFAULT_POLL_INTERVAL);
    macro_rules! or_push {
        ($path:expr, $id:expr, $r:expr) => {
            if let Err(e) = $r
                && !matches!(
                    e.kind,
                    notify::ErrorKind::PathNotFound | notify::ErrorKind::WatchNotFound
                )
            {
                push_error(&mut batch, $id, Some(ArcPath::from($path)), anyhow!(e))
            }
        };
    }
    macro_rules! add_watch {
        ($path:expr, $id:expr, on_success: $success:block) => {
            match watcher.watch($path, RecursiveMode::NonRecursive) {
                Ok(()) => $success,
                Err(e) => {
                    push_error(&mut batch, $id, Some(ArcPath::from($path)), anyhow!(e))
                }
            }
        };
    }
    macro_rules! status_change {
        ($id:expr, $syn_ok:expr) => {{
            let stc = watched.change_status($id).await;
            log::debug!("status change for {:?} {stc:?}", $id);
            if let Some(path) = stc.remove {
                if $syn_ok
                    && stc.syn
                    && let Some(w) = watched.by_id.get(&$id)
                    && w.watch.interest.intersects(
                        Interest::Delete
                        | Interest::DeleteFile
                        | Interest::DeleteFolder
                        | Interest::DeleteOther)
                {
                    let path = path.clone();
                    push_event(&mut batch, $id, path, EventKind::Event(Interest::Delete))
                }
                or_push!(path, $id, watcher.unwatch(&path));
            }
            match stc.add {
                None => (),
                Some(AddAction::AddWatch { path, .. }) => {
                    add_watch!(&path, $id, on_success: {
                        if let Some(w) = watched.by_id.get(&$id)
                            && w.watch.interest.intersects(
                                Interest::Create
                                | Interest::CreateFile
                                | Interest::CreateFolder
                                    | Interest::CreateOther)
                        {
                            let path = path.clone();
                            log::debug!("synthetic create for {:?} {}", $id, path.display());
                            push_event(&mut batch, $id, path, EventKind::Event(Interest::Create))
                        }
                    });
                }
                Some(AddAction::AddPending { watch_path, .. }) => {
                    add_watch!(&watch_path, $id, on_success: { () })
                }
            }
        }}
    }
    loop {
        select! {
            _ = poll_interval.tick() => {
                if watched.poll_batch() > 0 {
                    log::trace!("starting poll cycle");
                    for id in watched.poll_cycle().await.drain(..) {
                        status_change!(id, true)
                    }
                }
            },
            n = rx_notify.recv_many(&mut recv_buf, MAX_NOTIFY_BATCH) => {
                if n == 0 {
                    log::debug!("notify channel closed, exiting");
                    break
                }
                for ev in recv_buf.drain(..) {
                    let mut status = watched.process_event(&mut batch, ev).await;
                    for id in status.drain(..) {
                        status_change!(id, false)
                    }
                }
            },
            n = rx.recv_many(&mut cmd_buf, MAX_CMD_BATCH) => {
                if n == 0 {
                    log::debug!("command channel closed, exiting");
                    break
                }
                for cmd in cmd_buf.drain(..) {
                    log::debug!("processing watch command {cmd:?}");
                    match cmd {
                        Cmd::Watch(w) => {
                            let id = w.id;
                            let nev = EventKind::Event(Interest::Established);
                            let mut same_path = false;
                            if let (Some(old_w), Some(remove)) = watched.remove_watch(&id) {
                                if old_w.watch.path == w.path {
                                    same_path = true
                                } else {
                                    if let Err(e) = watcher.unwatch(&remove) {
                                        log::warn!("could not unwatch {} when switching {e:?}", remove.display())
                                    }
                                }
                            }
                            match watched.add_watch(w).await {
                                AddAction::AddWatch { path, notify } if !same_path => {
                                    add_watch!(&path, id, on_success: {
                                        if notify {
                                            push_event(&mut batch, id, path, nev)
                                        }
                                    })
                                }
                                AddAction::AddWatch { path, notify } => {
                                    if notify {
                                        push_event(&mut batch, id, path, nev)
                                    }
                                }
                                AddAction::AddPending { watch_path, full_path, notify } => {
                                    add_watch!(&watch_path, id, on_success: {
                                        if notify {
                                            push_event(&mut batch, id, full_path, nev);
                                        }
                                    })
                                }
                            }
                        },
                        Cmd::Stop(id) => match watched.remove_watch(&id).1 {
                            None => (),
                            Some(path) => {
                                or_push!(path, id, watcher.unwatch(&path))
                            }
                        },
                        Cmd::SetPollInterval(d) => {
                            poll_interval = tokio::time::interval(d);
                        }
                        Cmd::SetPollBatch(n) => {
                            watched.poll_batch = Some(n)
                        }
                    }
                }
            },
        }
        if !batch.is_empty() {
            log::debug!("sending event batch {batch:?}");
            if let Err(_) = tx.handle_event(batch).await {
                break;
            }
            batch = BATCH_POOL.take()
        }
    }
    let mut batch = BATCH_POOL.take();
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            Cmd::Stop(_) | Cmd::SetPollBatch(_) | Cmd::SetPollInterval(_) => (),
            Cmd::Watch(w) => {
                let e = anyhow!("the watcher thread has stopped");
                push_error(&mut batch, w.id, None, e);
            }
        }
    }
    if !batch.is_empty() {
        let _ = tx.handle_event(batch).await;
    }
    log::debug!("watch task exiting");
}
