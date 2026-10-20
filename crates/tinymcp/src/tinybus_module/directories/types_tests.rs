use super::*;

use std::sync::atomic::{AtomicBool, Ordering};

use tinybus::transport::memory::MemoryBus;
use tinymcp_bus::names;

async fn connected_service() -> (Connection, Arc<McpService>) {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Arc::new(McpService::new(&ModuleConfig::default()).unwrap());
    connection
        .serve_at(
            "/ai/tinyhumans/tinymcp/Test".try_into().unwrap(),
            service.dispatch_interface(),
        )
        .await
        .unwrap();
    connection.request_name(names::INTERFACE).await.unwrap();
    (connection, service)
}

#[test]
fn failed_serve_marks_transition_failed_and_runs_the_failure_hook() {
    let transitions = Arc::new(Mutex::new(HashMap::from([(
        "t1".to_string(),
        TransitionStatus::Pending,
    )])));
    let complete = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let complete_hook = Arc::clone(&complete);
    let failure_hook = Arc::clone(&failed);
    let mut callbacks = LifecycleCallbacks::new(
        Some(("t1".to_string(), Arc::clone(&transitions))),
        move || complete_hook.store(true, Ordering::Release),
        move || failure_hook.store(true, Ordering::Release),
    );

    callbacks.failed();
    callbacks.publish_status(TransitionStatus::Failed);

    assert!(!complete.load(Ordering::Acquire));
    assert!(failed.load(Ordering::Acquire));
    assert_eq!(transitions.lock().unwrap()["t1"], TransitionStatus::Failed);
}

#[tokio::test]
async fn failed_unserve_restores_dispatch_and_reports_failed_cleanup() {
    let (connection, service) = connected_service().await;
    let transitions = Arc::new(Mutex::new(HashMap::from([(
        "t1".to_string(),
        TransitionStatus::Pending,
    )])));
    let failed = Arc::new(AtomicBool::new(false));
    let failure_hook = Arc::clone(&failed);
    let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    let missing_path: tinybus::ObjectPath = "/ai/tinyhumans/tinymcp/NotServed".try_into().unwrap();
    schedule_unserve(
        connection.clone(),
        missing_path,
        Some(service),
        gate,
        LifecycleCallbacks::new(
            Some(("t1".to_string(), Arc::clone(&transitions))),
            || {},
            move || failure_hook.store(true, Ordering::Release),
        ),
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if transitions.lock().unwrap()["t1"] == TransitionStatus::Failed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(failed.load(Ordering::Acquire));
    let servers: Vec<serde_json::Value> = connection
        .proxy(
            names::INTERFACE,
            "/ai/tinyhumans/tinymcp/Test",
            names::INTERFACE,
        )
        .unwrap()
        .call(names::methods::STATIC_LIST, ())
        .await
        .unwrap();
    assert_eq!(servers, Vec::<serde_json::Value>::new());
}

#[tokio::test]
async fn successful_unserve_completes_cleanup_and_removes_the_object() {
    let (connection, service) = connected_service().await;
    let transitions = Arc::new(Mutex::new(HashMap::from([(
        "t1".to_string(),
        TransitionStatus::Pending,
    )])));
    let path: tinybus::ObjectPath = "/ai/tinyhumans/tinymcp/Test".try_into().unwrap();
    let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    schedule_unserve(
        connection.clone(),
        path.clone(),
        Some(service),
        gate,
        LifecycleCallbacks::new(
            Some(("t1".to_string(), Arc::clone(&transitions))),
            || {},
            || {},
        ),
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while transitions.lock().unwrap()["t1"] == TransitionStatus::Pending {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(
        transitions.lock().unwrap()["t1"],
        TransitionStatus::Complete
    );
    assert!(
        connection
            .proxy(names::INTERFACE, &path, names::INTERFACE)
            .unwrap()
            .call::<Vec<serde_json::Value>>(names::methods::STATIC_LIST, ())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scheduled_serve_registers_and_replaces_before_completing_transition() {
    let (connection, _) = connected_service().await;
    let path: tinybus::ObjectPath = "/ai/tinyhumans/tinymcp/Lifecycle".try_into().unwrap();
    let original = Arc::new(McpService::new(&ModuleConfig::default()).unwrap());
    let first_status = Arc::new(Mutex::new(HashMap::from([(
        "t1".to_string(),
        TransitionStatus::Pending,
    )])));
    let first_gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    schedule_serve(
        connection.clone(),
        path.clone(),
        Arc::clone(&original),
        None,
        first_gate,
        LifecycleCallbacks::new(
            Some(("t1".to_string(), Arc::clone(&first_status))),
            || {},
            || {},
        ),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while first_status.lock().unwrap()["t1"] == TransitionStatus::Pending {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        first_status.lock().unwrap()["t1"],
        TransitionStatus::Complete
    );

    let replacement = Arc::new(McpService::new(&ModuleConfig::default()).unwrap());
    let replacement_status = Arc::new(Mutex::new(HashMap::from([(
        "t2".to_string(),
        TransitionStatus::Pending,
    )])));
    let replacement_gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    schedule_serve(
        connection.clone(),
        path.clone(),
        replacement,
        Some(original),
        replacement_gate,
        LifecycleCallbacks::new(
            Some(("t2".to_string(), Arc::clone(&replacement_status))),
            || {},
            || {},
        ),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while replacement_status.lock().unwrap()["t2"] == TransitionStatus::Pending {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        replacement_status.lock().unwrap()["t2"],
        TransitionStatus::Complete
    );

    let servers: Vec<serde_json::Value> = connection
        .proxy(names::INTERFACE, &path, names::INTERFACE)
        .unwrap()
        .call(names::methods::STATIC_LIST, ())
        .await
        .unwrap();
    assert_eq!(servers, Vec::<serde_json::Value>::new());
}

#[tokio::test]
async fn an_inflight_close_is_reused_and_missing_directories_are_already_closed() {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let opener = Arc::new(
        DirectoryOpener::new(
            connection,
            &ModuleConfig::default(),
            SupervisorConfig::default(),
            crate::storage::installed_provider(),
        )
        .unwrap(),
    );
    let pending_dir = tempfile::tempdir().unwrap();
    opener.mark_pending(
        pending_dir.path(),
        PendingTransition {
            id: "t1".to_string(),
            object_path: format!("{DIRECTORY_OBJECT_PREFIX}/d1"),
            client: None,
            kind: PendingTransitionKind::Close,
        },
    );

    assert_eq!(
        opener
            .close(pending_dir.path().to_str().unwrap())
            .await
            .unwrap(),
        Some("t1".to_string())
    );
    assert!(matches!(
        opener.open(pending_dir.path().to_str().unwrap()).await,
        Err(Error::InvalidArgument { .. })
    ));
    let missing_dir = tempfile::tempdir().unwrap();
    assert_eq!(
        opener
            .close(missing_dir.path().to_str().unwrap())
            .await
            .unwrap(),
        None
    );
}

#[test]
fn pending_transitions_cannot_be_evicted_and_closed_observations_are_pruned_oldest_first() {
    let mut pending = (0..MAX_TRANSITION_OBSERVATIONS)
        .map(|index| (format!("t{index}"), TransitionStatus::Pending))
        .collect::<HashMap<_, _>>();
    prune_transition_observations(&mut pending);
    assert_eq!(pending.len(), MAX_TRANSITION_OBSERVATIONS);

    let mut transitions = (0..100)
        .map(|index| {
            (
                format!("t{index}"),
                if index % 2 == 0 {
                    TransitionStatus::Complete
                } else {
                    TransitionStatus::Failed
                },
            )
        })
        .chain(
            (100..MAX_TRANSITION_OBSERVATIONS)
                .map(|index| (format!("t{index}"), TransitionStatus::Pending)),
        )
        .collect::<HashMap<_, _>>();
    prune_transition_observations(&mut transitions);
    assert_eq!(transitions.len(), MAX_TRANSITION_OBSERVATIONS / 2 - 1);
    assert!(!transitions.contains_key("t0"));
    assert!(transitions.contains_key("t99"));
    assert!(transitions.contains_key("t100"));
    assert!(
        transitions
            .values()
            .any(|status| *status == TransitionStatus::Pending)
    );
    assert_eq!(next_nonzero(&AtomicU64::new(u64::MAX)), None);
}

#[tokio::test]
async fn a_full_pending_transition_registry_rejects_new_work_without_eviction() {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let opener = DirectoryOpener::new(
        connection,
        &ModuleConfig::default(),
        SupervisorConfig::default(),
        crate::storage::installed_provider(),
    )
    .unwrap();
    opener.transitions.lock().unwrap().extend(
        (1..=MAX_TRANSITION_OBSERVATIONS).map(|id| (format!("t{id}"), TransitionStatus::Pending)),
    );

    assert!(matches!(
        opener.new_transition_id(),
        Err(Error::InvalidArgument { .. })
    ));
    let transitions = opener.transitions.lock().unwrap();
    assert_eq!(transitions.len(), MAX_TRANSITION_OBSERVATIONS);
    assert_eq!(transitions["t1"], TransitionStatus::Pending);
}

#[tokio::test]
async fn exhausted_object_and_transition_id_spaces_are_refused_by_name() {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let opener = DirectoryOpener::new(
        connection,
        &ModuleConfig::default(),
        SupervisorConfig::default(),
        crate::storage::installed_provider(),
    )
    .unwrap();
    opener.next_object_id.store(u64::MAX, Ordering::Relaxed);
    opener.next_transition_id.store(u64::MAX, Ordering::Relaxed);

    assert!(matches!(
        opener.allocate_object_path(),
        Err(Error::InvalidArgument { .. })
    ));
    assert!(matches!(
        opener.new_transition_id(),
        Err(Error::InvalidArgument { .. })
    ));
    assert!(matches!(
        opener.completed_transition_id(),
        Err(Error::InvalidArgument { .. })
    ));
}
