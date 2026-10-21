//! The bounded directory registry and its asynchronous lifecycle transitions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tinybus::Connection;
use tinymcp_bus::{
    DIRECTORY_OBJECT_PREFIX, McpClientConfig, OBJECT_PATH, OpenConfiguredResponse, TransitionStatus,
};

use crate::error::{Error, Result};
use crate::registry::SupervisorConfig;
use crate::storage::StorageProvider;
use crate::tinybus_module::{McpService, ModuleConfig};

/// How many active directories one module process will serve.
pub(in crate::tinybus_module) const MAX_OPEN_DIRECTORIES: usize = 32;

/// Completed transition observations retained for a host that retries a lost
/// status reply. Pending transitions are never evicted.
const MAX_TRANSITION_OBSERVATIONS: usize = 128;

type TransitionStates = Arc<Mutex<HashMap<String, TransitionStatus>>>;
type TransitionNotice = Option<(String, TransitionStates)>;
type LifecycleHook = Box<dyn FnOnce() + Send + 'static>;

struct LifecycleCallbacks {
    transition: TransitionNotice,
    on_complete: Option<LifecycleHook>,
    on_failure: Option<LifecycleHook>,
}

impl LifecycleCallbacks {
    fn new(
        transition: TransitionNotice,
        on_complete: impl FnOnce() + Send + 'static,
        on_failure: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            transition,
            on_complete: Some(Box::new(on_complete)),
            on_failure: Some(Box::new(on_failure)),
        }
    }

    fn completed(&mut self) {
        if let Some(on_complete) = self.on_complete.take() {
            on_complete();
        }
    }

    fn failed(&mut self) {
        if let Some(on_failure) = self.on_failure.take() {
            on_failure();
        }
    }

    fn publish_status(&mut self, status: TransitionStatus) {
        set_status(self.transition.take(), status);
    }
}

#[derive(Clone)]
struct ServedDirectory {
    object_path: String,
    client: McpClientConfig,
    service: ServiceOwner,
}

#[derive(Clone)]
struct PendingTransition {
    id: String,
    object_path: String,
    client: Option<McpClientConfig>,
    kind: PendingTransitionKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingTransitionKind {
    Open,
    OpenConfigured,
    Close,
}

#[derive(Clone)]
enum ServiceOwner {
    Root(Weak<McpService>),
    Directory(Arc<McpService>),
}

struct ExistingDirectory {
    path: Option<String>,
    old_service: Option<Arc<McpService>>,
    is_root: bool,
    unchanged: bool,
}

impl ServiceOwner {
    fn upgrade(&self) -> Option<Arc<McpService>> {
        match self {
            Self::Root(service) => service.upgrade(),
            Self::Directory(service) => Some(Arc::clone(service)),
        }
    }
}

/// The root object's ability to serve additional directories.
pub(in crate::tinybus_module) struct DirectoryOpener {
    connection: Connection,
    /// The root module's host-bound provider, reused by every opened directory.
    provider: Arc<dyn StorageProvider>,
    /// The client configuration used by legacy `Open(data_dir)`.
    client: Arc<std::sync::RwLock<McpClientConfig>>,
    supervisor: SupervisorConfig,
    /// Active directories, including entries whose close is still pending.
    served: Arc<Mutex<HashMap<PathBuf, ServedDirectory>>>,
    /// Held by one transition worker until replacement and cleanup finish.
    /// Calls never await this lock while dispatch holds `TinyBus`'s object-tree
    /// read lock; a competing mutation fails closed and the host retries after
    /// observing its transition status.
    transition_gate: Arc<tokio::sync::Mutex<()>>,
    next_object_id: Arc<AtomicU64>,
    next_transition_id: Arc<AtomicU64>,
    transitions: TransitionStates,
    pending_by_directory: Arc<Mutex<HashMap<PathBuf, PendingTransition>>>,
}

impl std::fmt::Debug for DirectoryOpener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectoryOpener")
            .field("supervisor", &self.supervisor)
            .finish_non_exhaustive()
    }
}

impl DirectoryOpener {
    /// Builds the opener for a module loaded with `config`.
    ///
    /// The load-time directory, when there is one, is already served at the
    /// root path, so it is recorded there rather than opened a second time.
    pub(in crate::tinybus_module) fn new(
        connection: Connection,
        config: &ModuleConfig,
        supervisor: SupervisorConfig,
        provider: Arc<dyn StorageProvider>,
    ) -> Result<Self> {
        let mut served = HashMap::new();
        if let Some(dir) = &config.data_dir {
            let absolute = super::super::service::absolute_data_dir(dir)?;
            served.insert(
                absolute,
                ServedDirectory {
                    object_path: OBJECT_PATH.to_string(),
                    client: config.client.clone(),
                    service: ServiceOwner::Root(Weak::new()),
                },
            );
        }

        Ok(Self {
            connection,
            provider,
            client: Arc::new(std::sync::RwLock::new(config.client.clone())),
            supervisor,
            served: Arc::new(Mutex::new(served)),
            transition_gate: Arc::new(tokio::sync::Mutex::new(())),
            next_object_id: Arc::new(AtomicU64::new(0)),
            next_transition_id: Arc::new(AtomicU64::new(0)),
            transitions: Arc::new(Mutex::new(HashMap::new())),
            pending_by_directory: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Serves `data_dir` as its own object and returns the object path.
    ///
    /// Idempotent per directory. A concurrent lifecycle transition returns an
    /// invalid-argument error so the caller can poll its status and retry.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] when the path is empty, relative, a
    /// transition is already active, or the active-object limit is reached;
    /// [`Error::Bus`] when the object cannot be served, plus errors from
    /// building the service. A failed open leaves no new entry recorded.
    pub(in crate::tinybus_module) async fn open(&self, data_dir: &str) -> Result<String> {
        let dir = checked_directory(data_dir)?;
        if let Some(pending) = self.pending_for(&dir) {
            if pending.kind != PendingTransitionKind::Close {
                return Ok(pending.object_path);
            }
            return Err(Error::invalid_argument(
                "the data directory is being closed",
            ));
        }
        let client = self
            .client
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.open_with_client(data_dir, &client, false, None)
            .await
            .map(|response| response.object_path)
    }

    /// Serves `data_dir` with the supplied host-specific client settings.
    ///
    /// A changed configuration replaces the object at its current path. The
    /// returned transition id remains pending until the new object is served
    /// and the old service's maintenance, protocol operations, and dynamic
    /// connections have stopped. Hosts poll `TransitionStatus`; this method
    /// does not wait while `TinyBus` holds the root object's dispatch read lock.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for an invalid path, active
    /// transition, unavailable root service, or exhausted directory bound.
    /// Storage, client, and bus-serving errors are returned unchanged.
    pub(in crate::tinybus_module) async fn open_configured(
        self: &Arc<Self>,
        data_dir: &str,
        client: &McpClientConfig,
    ) -> Result<OpenConfiguredResponse> {
        self.open_with_client(data_dir, client, true, Some(Arc::clone(self)))
            .await
    }

    /// Closes one active directory and returns its cleanup transition id.
    ///
    /// `None` means that directory has already been fully closed. Active
    /// capacity remains reserved until `TransitionStatus` reports completion.
    /// The root object is kept alive as an empty in-memory service so it can
    /// continue accepting lifecycle calls after sign-out.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] for an invalid path or a concurrent
    /// lifecycle transition, plus errors while constructing the reset root.
    pub(in crate::tinybus_module) async fn close(
        self: &Arc<Self>,
        data_dir: &str,
    ) -> Result<Option<String>> {
        let dir = checked_directory(data_dir)?;
        if let Some(pending) = self.pending_for(&dir) {
            if pending.kind == PendingTransitionKind::Close {
                return Ok(Some(pending.id));
            }
            return Err(Error::invalid_argument(
                "a directory configuration transition is pending",
            ));
        }
        let gate = self.try_transition_guard()?;
        let (path, old_service, is_root) = {
            let served = self
                .served
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(existing) = served.get(&dir) else {
                return Ok(None);
            };
            let is_root = existing.object_path == OBJECT_PATH;
            let old_service = existing.service.upgrade().ok_or_else(|| {
                Error::invalid_argument("the existing data directory service is unavailable")
            })?;
            (existing.object_path.clone(), old_service, is_root)
        };

        let reset_root = if is_root {
            Some(Arc::new(
                self.new_service(None, McpClientConfig::default())?
                    .with_maintenance(self.supervisor.clone())
                    .await
                    .with_directory_opener(Arc::clone(self)),
            ))
        } else {
            None
        };
        let object_path = path.as_str().try_into().map_err(|error| Error::Bus {
            detail: format!("could not name the directory object: {error}"),
        })?;
        let transition_id = self.new_transition_id()?;
        self.mark_pending(
            &dir,
            PendingTransition {
                id: transition_id.clone(),
                object_path: path.clone(),
                client: None,
                kind: PendingTransitionKind::Close,
            },
        );
        if is_root {
            let reset = reset_root
                .ok_or_else(|| Error::invalid_argument("the root reset service is unavailable"))?;
            self.schedule_root_close(
                dir,
                object_path,
                old_service,
                reset,
                gate,
                transition_id.clone(),
            )
            .await;
        } else {
            self.schedule_child_close(dir, old_service, object_path, gate, transition_id.clone());
        }
        Ok(Some(transition_id))
    }

    async fn schedule_root_close(
        &self,
        dir: PathBuf,
        object_path: tinybus::ObjectPath,
        old_service: Arc<McpService>,
        reset: Arc<McpService>,
        gate: tokio::sync::OwnedMutexGuard<()>,
        transition_id: String,
    ) {
        let transitions = Arc::clone(&self.transitions);
        let client = Arc::clone(&self.client);
        let served_on_complete = Arc::clone(&self.served);
        let served_on_failure = Arc::clone(&self.served);
        let pending_on_complete = Arc::clone(&self.pending_by_directory);
        let pending_on_failure = Arc::clone(&self.pending_by_directory);
        let old_service_weak = Arc::downgrade(&old_service);
        let pending_complete_id = transition_id.clone();
        let pending_failure_id = transition_id.clone();
        let complete_path = dir.clone();
        let failure_path = dir.clone();
        schedule_serve(
            self.connection.clone(),
            object_path,
            reset,
            Some(old_service),
            gate,
            LifecycleCallbacks::new(
                Some((transition_id, transitions)),
                move || {
                    served_on_complete
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&complete_path);
                    *client
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        McpClientConfig::default();
                    remove_pending(&pending_on_complete, &complete_path, &pending_complete_id);
                },
                move || {
                    if let Some(old) = old_service_weak.upgrade()
                        && let Some(root) = served_on_failure
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_mut(&failure_path)
                    {
                        root.service = ServiceOwner::Root(Arc::downgrade(&old));
                    }
                    remove_pending(&pending_on_failure, &failure_path, &pending_failure_id);
                },
            ),
        )
        .await;
    }

    fn schedule_child_close(
        &self,
        dir: PathBuf,
        old_service: Arc<McpService>,
        object_path: tinybus::ObjectPath,
        gate: tokio::sync::OwnedMutexGuard<()>,
        transition_id: String,
    ) {
        let served = Arc::clone(&self.served);
        let pending_on_complete = Arc::clone(&self.pending_by_directory);
        let pending_on_failure = Arc::clone(&self.pending_by_directory);
        let pending_complete_id = transition_id.clone();
        let pending_failure_id = transition_id.clone();
        let complete_path = dir.clone();
        schedule_unserve(
            self.connection.clone(),
            object_path,
            Some(old_service),
            gate,
            LifecycleCallbacks::new(
                Some((transition_id, Arc::clone(&self.transitions))),
                move || {
                    served
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&complete_path);
                    remove_pending(&pending_on_complete, &complete_path, &pending_complete_id);
                },
                move || {
                    remove_pending(&pending_on_failure, &dir, &pending_failure_id);
                },
            ),
        );
    }

    /// Returns a nonblocking observation of a pending or recently completed
    /// transition.
    pub(in crate::tinybus_module) fn transition_status(&self, id: &str) -> TransitionStatus {
        self.transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .copied()
            .unwrap_or(TransitionStatus::Unknown)
    }

    fn pending_for(&self, directory: &Path) -> Option<PendingTransition> {
        self.pending_by_directory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(directory)
            .cloned()
    }

    fn mark_pending(&self, directory: &Path, pending: PendingTransition) {
        self.pending_by_directory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(directory.to_path_buf(), pending);
    }

    async fn open_with_client(
        &self,
        data_dir: &str,
        client: &McpClientConfig,
        reconfigure: bool,
        root_opener: Option<Arc<Self>>,
    ) -> Result<OpenConfiguredResponse> {
        let dir = checked_directory(data_dir)?;
        if let Some(pending) = self.pending_for(&dir) {
            if pending.kind == PendingTransitionKind::OpenConfigured
                && pending.client.as_ref() == Some(client)
            {
                return Ok(OpenConfiguredResponse {
                    object_path: pending.object_path,
                    transition_id: pending.id,
                });
            }
            return Err(Error::invalid_argument(
                "a directory lifecycle transition is pending",
            ));
        }
        let gate = self.try_transition_guard()?;
        let existing = self.existing_directory(&dir, client, reconfigure)?;
        if existing.unchanged {
            return Ok(OpenConfiguredResponse {
                object_path: existing
                    .path
                    .ok_or_else(|| Error::invalid_argument("the directory path is unavailable"))?,
                transition_id: self.completed_transition_id()?,
            });
        }
        let service = self
            .configured_service(&dir, client, existing.is_root, root_opener)
            .await?;
        let path = match existing.path {
            Some(path) => path,
            None => self.allocate_object_path()?,
        };
        let object_path = path.as_str().try_into().map_err(|error| Error::Bus {
            detail: format!("could not name an object for the directory: {error}"),
        })?;
        let transition_id = self.new_transition_id()?;
        self.mark_pending(
            &dir,
            PendingTransition {
                id: transition_id.clone(),
                object_path: path.clone(),
                client: reconfigure.then(|| client.clone()),
                kind: if reconfigure {
                    PendingTransitionKind::OpenConfigured
                } else {
                    PendingTransitionKind::Open
                },
            },
        );
        let service_owner = if existing.is_root {
            ServiceOwner::Root(Arc::downgrade(&service))
        } else {
            ServiceOwner::Directory(Arc::clone(&service))
        };
        let return_path = path.clone();
        let previous = self
            .served
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&dir)
            .cloned();
        let callbacks = self.open_callbacks(
            dir,
            path,
            client.clone(),
            service_owner,
            previous,
            transition_id.clone(),
        );
        schedule_serve(
            self.connection.clone(),
            object_path,
            service,
            existing.old_service,
            gate,
            callbacks,
        )
        .await;

        Ok(OpenConfiguredResponse {
            object_path: return_path,
            transition_id,
        })
    }

    fn existing_directory(
        &self,
        dir: &Path,
        client: &McpClientConfig,
        reconfigure: bool,
    ) -> Result<ExistingDirectory> {
        let served = self
            .served
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let existing = served.get(dir);
        let unchanged = existing.is_some_and(|entry| !reconfigure || entry.client == *client);
        if existing.is_none() && served.len() >= MAX_OPEN_DIRECTORIES {
            return Err(Error::invalid_argument(format!(
                "this module already serves the maximum of {MAX_OPEN_DIRECTORIES} data directories"
            )));
        }
        let is_root = existing.is_some_and(|entry| entry.object_path == OBJECT_PATH);
        let old_service = existing.and_then(|entry| entry.service.upgrade());
        if reconfigure && existing.is_some() && old_service.is_none() {
            return Err(Error::invalid_argument(
                "the existing data directory service is no longer available",
            ));
        }
        Ok(ExistingDirectory {
            path: existing.map(|entry| entry.object_path.clone()),
            old_service,
            is_root,
            unchanged,
        })
    }

    /// A service whose stdio servers start through this opener's bus
    /// connection: in the jail when the client configures one.
    fn new_service(
        &self,
        data_dir: Option<PathBuf>,
        client: McpClientConfig,
    ) -> Result<McpService> {
        let stdio = crate::tinybus_module::stdio_launchers(Some(&self.connection), &client);
        McpService::with_provider_and_stdio(
            &ModuleConfig {
                data_dir,
                client,
                storage_descriptor: None,
            },
            Arc::clone(&self.provider),
            stdio,
        )
    }

    async fn configured_service(
        &self,
        dir: &Path,
        client: &McpClientConfig,
        is_root: bool,
        root_opener: Option<Arc<Self>>,
    ) -> Result<Arc<McpService>> {
        let mut service = self
            .new_service(Some(dir.to_path_buf()), client.clone())?
            .with_maintenance(self.supervisor.clone())
            .await;
        if is_root {
            let opener = root_opener.ok_or_else(|| {
                Error::invalid_argument("the root opener is not available for reconfiguration")
            })?;
            service = service.with_directory_opener(opener);
        }
        Ok(Arc::new(service))
    }

    fn open_callbacks(
        &self,
        dir: PathBuf,
        path: String,
        client: McpClientConfig,
        service_owner: ServiceOwner,
        previous: Option<ServedDirectory>,
        transition_id: String,
    ) -> LifecycleCallbacks {
        let served = Arc::clone(&self.served);
        let served_on_failure = Arc::clone(&self.served);
        let pending_on_complete = Arc::clone(&self.pending_by_directory);
        let pending_on_failure = Arc::clone(&self.pending_by_directory);
        let pending_complete_id = transition_id.clone();
        let pending_failure_id = transition_id.clone();
        let complete_path = dir.clone();
        LifecycleCallbacks::new(
            Some((transition_id, Arc::clone(&self.transitions))),
            move || {
                served
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(
                        complete_path.clone(),
                        ServedDirectory {
                            object_path: path,
                            client,
                            service: service_owner,
                        },
                    );
                remove_pending(&pending_on_complete, &complete_path, &pending_complete_id);
            },
            move || {
                let mut map = served_on_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(previous) = previous {
                    map.insert(dir.clone(), previous);
                } else {
                    map.remove(&dir);
                }
                remove_pending(&pending_on_failure, &dir, &pending_failure_id);
            },
        )
    }

    fn try_transition_guard(&self) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        self.transition_gate
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::invalid_argument("a directory lifecycle transition is pending"))
    }

    fn allocate_object_path(&self) -> Result<String> {
        let id = next_nonzero(&self.next_object_id)
            .ok_or_else(|| Error::invalid_argument("the directory object id space is exhausted"))?;
        Ok(format!("{DIRECTORY_OBJECT_PREFIX}/d{id}"))
    }

    fn new_transition_id(&self) -> Result<String> {
        let id = next_nonzero(&self.next_transition_id)
            .ok_or_else(|| Error::invalid_argument("the transition id space is exhausted"))?;
        let id = format!("t{id}");
        let mut transitions = self
            .transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_transition_observations(&mut transitions);
        if transitions.len() >= MAX_TRANSITION_OBSERVATIONS {
            return Err(Error::invalid_argument(
                "too many unobserved directory transitions",
            ));
        }
        transitions.insert(id.clone(), TransitionStatus::Pending);
        Ok(id)
    }

    fn completed_transition_id(&self) -> Result<String> {
        let id = self.new_transition_id()?;
        self.set_transition_status(&id, TransitionStatus::Complete);
        Ok(id)
    }

    fn set_transition_status(&self, id: &str, status: TransitionStatus) {
        self.transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_string(), status);
    }

    pub(in crate::tinybus_module) fn register_root(&self, service: &Arc<McpService>) {
        let mut served = self
            .served
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(root) = served
            .values_mut()
            .find(|entry| entry.object_path == OBJECT_PATH)
        {
            root.service = ServiceOwner::Root(Arc::downgrade(service));
        }
    }
}

fn checked_directory(data_dir: &str) -> Result<PathBuf> {
    let dir = Path::new(data_dir);
    if data_dir.is_empty() || !dir.is_absolute() {
        return Err(Error::invalid_argument(
            "a data directory must be a non-empty absolute path",
        ));
    }
    Ok(dir.to_path_buf())
}

fn next_nonzero(counter: &AtomicU64) -> Option<u64> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.checked_add(1)?;
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some(next),
            Err(observed) => current = observed,
        }
    }
}

fn prune_transition_observations(transitions: &mut HashMap<String, TransitionStatus>) {
    let completed = transitions
        .iter()
        .filter_map(|(id, status)| {
            matches!(
                status,
                TransitionStatus::Complete | TransitionStatus::Failed
            )
            .then(|| {
                id.strip_prefix('t')
                    .and_then(|number| number.parse::<u64>().ok())
                    .map(|number| (number, id.clone()))
            })
            .flatten()
        })
        .collect::<Vec<_>>();
    if transitions.len() < MAX_TRANSITION_OBSERVATIONS {
        return;
    }
    let mut completed = completed;
    completed.sort_unstable_by_key(|(number, _)| *number);
    for (_, id) in completed {
        if transitions.len() < MAX_TRANSITION_OBSERVATIONS / 2 {
            break;
        }
        transitions.remove(&id);
    }
}

fn remove_pending(
    pending: &Mutex<HashMap<PathBuf, PendingTransition>>,
    directory: &Path,
    id: &str,
) {
    let mut pending = pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if pending
        .get(directory)
        .is_some_and(|transition| transition.id == id)
    {
        pending.remove(directory);
    }
}

async fn schedule_serve(
    connection: Connection,
    path: tinybus::ObjectPath,
    service: Arc<McpService>,
    replaced: Option<Arc<McpService>>,
    gate: tokio::sync::OwnedMutexGuard<()>,
    mut callbacks: LifecycleCallbacks,
) {
    let dispatch = service.dispatch_interface();
    let mut serve = Box::pin(async move { connection.serve_at(path, dispatch).await });

    // For a new path there is no old handler to drain. Queue the object-tree
    // write before replying so a caller can use the returned path immediately.
    // Replacements wait for the per-service gate first, keeping status calls
    // dispatchable while active handlers drain.
    if replaced.is_none() {
        let first_poll =
            std::future::poll_fn(|context| std::task::Poll::Ready(serve.as_mut().poll(context)))
                .await;
        if let std::task::Poll::Ready(result) = first_poll {
            match result {
                Ok(()) => {
                    callbacks.completed();
                    drop(gate);
                    callbacks.publish_status(TransitionStatus::Complete);
                }
                Err(error) => {
                    callbacks.failed();
                    tracing::error!("could not serve a directory lifecycle transition: {error}");
                    drop(gate);
                    callbacks.publish_status(TransitionStatus::Failed);
                }
            }
            return;
        }
    }

    tokio::spawn(async move {
        if let Some(replaced) = &replaced {
            replaced.quiesce().await;
        }
        match serve.await {
            Ok(()) => {
                if let Some(replaced) = replaced {
                    replaced.shutdown().await;
                }
                callbacks.completed();
                drop(gate);
                callbacks.publish_status(TransitionStatus::Complete);
            }
            Err(error) => {
                if let Some(replaced) = &replaced {
                    replaced.resume_dispatch().await;
                }
                callbacks.failed();
                tracing::error!("could not serve a directory lifecycle transition: {error}");
                drop(gate);
                callbacks.publish_status(TransitionStatus::Failed);
            }
        }
    });
}

fn schedule_unserve(
    connection: Connection,
    path: tinybus::ObjectPath,
    old_service: Option<Arc<McpService>>,
    gate: tokio::sync::OwnedMutexGuard<()>,
    mut callbacks: LifecycleCallbacks,
) {
    tokio::spawn(async move {
        if let Some(old_service) = &old_service {
            old_service.quiesce().await;
        }
        if connection.unserve(&path).await {
            if let Some(old_service) = old_service {
                old_service.shutdown().await;
            }
            callbacks.completed();
            drop(gate);
            callbacks.publish_status(TransitionStatus::Complete);
        } else {
            if let Some(old_service) = &old_service {
                old_service.resume_dispatch().await;
            }
            callbacks.failed();
            drop(gate);
            callbacks.publish_status(TransitionStatus::Failed);
        }
    });
}

fn set_status(transition: TransitionNotice, status: TransitionStatus) {
    if let Some((id, transitions)) = transition {
        transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, status);
    }
}

/// Converts a failed bus export into the crate's bus error.
#[cfg(test)]
pub(super) fn serving_error(error: impl std::fmt::Display) -> Error {
    Error::Bus {
        detail: format!("could not serve the directory: {error}"),
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod lifecycle_tests;
