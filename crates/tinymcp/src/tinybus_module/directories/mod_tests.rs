//! Unit tests for serving additional data directories.
//!
//! These run over a real in-memory bus: the property under test is that a path
//! `Open` returns is one a caller can actually reach, and that what it reaches
//! is isolated from every other directory. A test that only looked at the
//! returned string would agree with itself whatever the module did.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Error as BusError};
use tinymcp_bus::{
    DIRECTORY_OBJECT_PREFIX, INTERFACE, McpClientConfig, McpServerConfig, OBJECT_PATH,
    OpenConfiguredResponse, TransitionStatus, errors,
};

use super::MAX_OPEN_DIRECTORIES;
use crate::registry::SupervisorConfig;
use crate::tinybus_module::{McpService, ModuleConfig, test_linked_module};

/// Owns the host and broker alongside a connection so the linked test module
/// stays alive and receives the same attestation as production-linked code.
struct TestBus {
    connection: Connection,
    _host: tinybus::module::ModuleHost,
    _broker_task: tokio::task::JoinHandle<tinybus::Result<()>>,
}

impl std::ops::Deref for TestBus {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

/// Attaches the real linked module on a fresh in-memory bus. This exercises
/// confidential request delivery through `TinyBus`'s normal attestation gate.
async fn serve_root(config: &ModuleConfig) -> TestBus {
    static INSTALL_ONCE: std::sync::Once = std::sync::Once::new();
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let broker_task = broker.spawn(bus.clone());
    // The linked module's data directories sit on the in-memory provider: an
    // in-process bus has no attested storage host to give it secrets.
    // Installed once: the slot is process-wide, so replacing it per test would
    // swap the store out from under tests running in parallel.
    INSTALL_ONCE.call_once(|| {
        crate::storage::install_provider(
            std::sync::Arc::new(crate::storage::MemoryProvider::new()),
        );
    });
    let host = tinybus::module::ModuleHost::new(broker.clone());
    let module = test_linked_module().unwrap();
    let info = host
        .attach_linked_with_config(module, serde_json::to_value(config).unwrap())
        .unwrap();
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attestation = connection
        .attestation(info.manifest.bus_name)
        .await
        .unwrap();
    assert!(attestation.is_some(), "linked TinyMCP module is attested");

    TestBus {
        connection,
        _host: host,
        _broker_task: broker_task,
    }
}

/// Serves a local, un-attested root for tests that exercise ordinary legacy
/// methods without sending configured client data.
async fn serve_untrusted_root(config: &ModuleConfig) -> Connection {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let provider = crate::storage::installed_provider();
    let service = std::sync::Arc::new(
        McpService::new(config)
            .unwrap()
            .with_opener(
                connection.clone(),
                config,
                SupervisorConfig::default(),
                provider,
            )
            .unwrap(),
    );
    service.register_root();
    connection
        .serve_at(
            OBJECT_PATH.try_into().unwrap(),
            service.dispatch_interface(),
        )
        .await
        .unwrap();
    connection.request_name(INTERFACE).await.unwrap();
    connection
}

/// Calls `member` on the object at `path`.
async fn call<R: serde::de::DeserializeOwned>(
    connection: &Connection,
    path: &str,
    member: &str,
    args: Value,
) -> Result<R, BusError> {
    connection
        .proxy(INTERFACE, path, INTERFACE)
        .unwrap()
        .call(member, args)
        .await
}

async fn call_confidential<R: serde::de::DeserializeOwned>(
    connection: &Connection,
    path: &str,
    member: &str,
    args: Value,
) -> Result<R, BusError> {
    connection
        .proxy(INTERFACE, path, INTERFACE)
        .unwrap()
        .call_confidential(member, args)
        .await
}

async fn wait_transition(connection: &Connection, id: &str) {
    use tokio::time::{Duration, timeout};

    timeout(Duration::from_secs(5), async {
        loop {
            let status: TransitionStatus =
                call(connection, OBJECT_PATH, "TransitionStatus", json!([id]))
                    .await
                    .unwrap();
            match status {
                TransitionStatus::Pending => tokio::task::yield_now().await,
                TransitionStatus::Complete => return,
                TransitionStatus::Failed => panic!("directory transition failed"),
                TransitionStatus::Unknown => panic!("directory transition status was lost"),
                _ => panic!("unhandled directory transition status"),
            }
        }
    })
    .await
    .expect("directory transition eventually completes");
}

async fn open_configured_complete(connection: &Connection, request: Value) -> String {
    let response: OpenConfiguredResponse =
        call_confidential(connection, OBJECT_PATH, "OpenConfigured", json!([request]))
            .await
            .unwrap();
    wait_transition(connection, &response.transition_id).await;
    response.object_path
}

async fn conflicting_open_is_rejected_while_transition_is_pending(
    connection: &Connection,
    data_dir: &std::path::Path,
    endpoint: &str,
) {
    use tokio::time::{Duration, timeout};

    let error = timeout(
        Duration::from_secs(3),
        call_confidential::<OpenConfiguredResponse>(
            connection,
            OBJECT_PATH,
            "OpenConfigured",
            json!([configured_open(data_dir, "gamma", endpoint)]),
        ),
    )
    .await
    .expect("a conflicting pending reconfiguration is rejected without waiting")
    .unwrap_err();
    assert!(error.to_string().contains("pending"));
}

fn config_in(dir: &std::path::Path) -> ModuleConfig {
    ModuleConfig {
        storage_descriptor: None,
        data_dir: Some(dir.to_path_buf()),
        ..ModuleConfig::default()
    }
}

/// The name a failed call travelled under.
fn failure_name(error: &BusError) -> String {
    match error {
        BusError::MethodFailed { name, .. } => name.clone(),
        other => panic!("expected a method failure, got {other:?}"),
    }
}

fn audit_record(tool: &str) -> Value {
    json!({
        "timestamp_ms": 1_000,
        "client_info": "test",
        "tool_name": tool,
        "args_summary": {},
        "success": true,
    })
}

async fn audit_tools(connection: &Connection, path: &str) -> Vec<String> {
    let records: Vec<Value> = call(connection, path, "AuditListWrites", json!([{}]))
        .await
        .unwrap();
    records
        .iter()
        .map(|record| record["tool_name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn opening_a_directory_returns_a_path_that_answers() {
    let root_dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;

    let path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([other.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    assert!(path.starts_with(DIRECTORY_OBJECT_PREFIX), "{path}");
    let installed: Vec<Value> = call(&connection, &path, "InstalledList", json!([]))
        .await
        .unwrap();
    assert_eq!(installed.len(), 0);
}

#[tokio::test]
async fn configured_directories_keep_their_own_mocked_static_servers() {
    let root_dir = tempfile::tempdir().unwrap();
    let (alpha_dir, beta_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let alpha_url = mock_static_server("alpha provider").await;
    let beta_url = mock_static_server("beta provider").await;
    let connection = serve_root(&config_in(root_dir.path())).await;

    let alpha_path = open_configured_complete(
        &connection,
        configured_open(alpha_dir.path(), "alpha", &alpha_url),
    )
    .await;
    let beta_path = open_configured_complete(
        &connection,
        configured_open(beta_dir.path(), "beta", &beta_url),
    )
    .await;

    assert_ne!(alpha_path, beta_path);
    let alpha_names: Vec<String> = call(&connection, &alpha_path, "StaticList", json!([]))
        .await
        .unwrap();
    let beta_names: Vec<String> = call(&connection, &beta_path, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(alpha_names, ["alpha"]);
    assert_eq!(beta_names, ["beta"]);

    let alpha_result: Value = call(
        &connection,
        &alpha_path,
        "StaticCallTool",
        json!(["alpha", "identity", {}]),
    )
    .await
    .unwrap();
    let beta_result: Value = call(
        &connection,
        &beta_path,
        "StaticCallTool",
        json!(["beta", "identity", {}]),
    )
    .await
    .unwrap();

    assert_eq!(
        alpha_result["rendered"]["content"][0]["text"],
        "alpha provider"
    );
    assert_eq!(
        beta_result["rendered"]["content"][0]["text"],
        "beta provider"
    );
}

#[tokio::test]
async fn configured_open_requires_confidential_delivery() {
    let root_dir = tempfile::tempdir().unwrap();
    let child = tempfile::tempdir().unwrap();
    let endpoint = mock_static_server("provider").await;
    let connection = serve_root(&config_in(root_dir.path())).await;
    let error = call::<OpenConfiguredResponse>(
        &connection,
        OBJECT_PATH,
        "OpenConfigured",
        json!([configured_open(child.path(), "provider", &endpoint)]),
    )
    .await
    .unwrap_err();
    assert_eq!(
        failure_name(&error),
        BusError::CONFIDENTIALITY_REQUIRED,
        "plain OpenConfigured call was not refused for confidentiality: {error:?}"
    );
}

#[tokio::test]
async fn reopening_one_directory_with_different_client_settings_replaces_its_service() {
    let root_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let alpha_url = mock_static_server("alpha provider").await;
    let beta_url = mock_static_server("beta provider").await;
    let connection = serve_root(&config_in(root_dir.path())).await;
    let first_config = configured_open(data_dir.path(), "alpha", &alpha_url);
    let first = open_configured_complete(&connection, first_config).await;
    let same_path = open_configured_complete(
        &connection,
        configured_open(data_dir.path(), "alpha", &alpha_url),
    )
    .await;
    let updated_path = open_configured_complete(
        &connection,
        configured_open(data_dir.path(), "beta", &beta_url),
    )
    .await;
    let first_path = first;

    assert_eq!(first_path, same_path);
    let names: Vec<String> = call(&connection, &first_path, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(first_path, updated_path);
    assert_eq!(names, ["beta"]);
    let result: Value = call(
        &connection,
        &updated_path,
        "StaticCallTool",
        json!(["beta", "identity", {}]),
    )
    .await
    .unwrap();
    assert_eq!(result["rendered"]["content"][0]["text"], "beta provider");
}

#[tokio::test]
async fn reconfiguring_the_load_time_directory_keeps_the_root_opener() {
    let root_dir = tempfile::tempdir().unwrap();
    let (alpha_url, beta_url) = (
        mock_static_server("alpha provider").await,
        mock_static_server("beta provider").await,
    );
    let config = ModuleConfig {
        storage_descriptor: None,
        client: configured_client("alpha", &alpha_url),
        ..config_in(root_dir.path())
    };
    let connection = serve_root(&config).await;

    let root_path = open_configured_complete(
        &connection,
        configured_open(root_dir.path(), "beta", &beta_url),
    )
    .await;
    assert_eq!(root_path, OBJECT_PATH);

    let names: Vec<String> = call(&connection, OBJECT_PATH, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(names, ["beta"]);

    let legacy_path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([root_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert_eq!(legacy_path, OBJECT_PATH);

    let child_dir = tempfile::tempdir().unwrap();
    let child_path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([child_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    let child_names: Vec<String> = call(&connection, &child_path, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(child_names, ["alpha"]);
}

#[tokio::test]
async fn reconfiguration_waits_for_an_in_flight_tool_call_before_replacing_the_service() {
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::time::{Duration, timeout};

    let root_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(ToolCallGate {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let alpha_url = mock_static_server_with_gate("alpha provider", Some(Arc::clone(&gate))).await;
    let beta_url = mock_static_server("beta provider").await;
    let connection = serve_root(&config_in(root_dir.path())).await;
    let response: OpenConfiguredResponse = call_confidential(
        &connection,
        OBJECT_PATH,
        "OpenConfigured",
        json!([configured_open(data_dir.path(), "alpha", &alpha_url)]),
    )
    .await
    .unwrap();
    wait_transition(&connection, &response.transition_id).await;
    let path = response.object_path;

    let active_connection = connection.clone();
    let active_path = path.clone();
    let active_call = tokio::spawn(async move {
        call::<Value>(
            &active_connection,
            &active_path,
            "StaticCallTool",
            json!(["alpha", "identity", {}]),
        )
        .await
    });
    // This is only the fixture's readiness barrier, not the latency being
    // tested below. CI hosts can be heavily loaded while the in-process HTTP
    // server and module bus are starting, so give that setup more headroom.
    let started = timeout(Duration::from_secs(15), gate.started.acquire())
        .await
        .expect("the old static tool call reaches its provider")
        .unwrap();
    started.forget();

    let update_connection = connection.clone();
    let update_path = path.clone();
    let update_data_dir = data_dir.path().to_path_buf();
    let update = tokio::spawn(async move {
        call_confidential::<OpenConfiguredResponse>(
            &update_connection,
            OBJECT_PATH,
            "OpenConfigured",
            json!([configured_open(&update_data_dir, "beta", &beta_url)]),
        )
        .await
    });
    let updated = timeout(Duration::from_secs(3), update)
        .await
        .expect("reconfiguration replies without waiting on the old object-tree reader")
        .unwrap()
        .unwrap();
    assert_eq!(updated.object_path, update_path);
    assert_eq!(
        call::<TransitionStatus>(
            &connection,
            OBJECT_PATH,
            "TransitionStatus",
            json!([updated.transition_id.clone()]),
        )
        .await
        .unwrap(),
        TransitionStatus::Pending,
    );
    assert!(
        !active_call.is_finished(),
        "the old tool call ended before its release barrier: {:?}",
        active_call.await
    );

    gate.release.add_permits(1);
    let old_result = timeout(Duration::from_secs(3), active_call)
        .await
        .expect("the in-flight call completes before replacement")
        .unwrap()
        .unwrap();
    assert_eq!(
        old_result["rendered"]["content"][0]["text"],
        "alpha provider"
    );
    wait_transition(&connection, &updated.transition_id).await;

    let names: Vec<String> = call(&connection, &path, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(names, ["beta"]);
}

#[tokio::test]
async fn root_transition_status_is_nonblocking_before_tree_writer_registration() {
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::time::{Duration, timeout};

    let root_dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(ToolCallGate {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let alpha_url = mock_static_server_with_gate("alpha provider", Some(Arc::clone(&gate))).await;
    let beta_url = mock_static_server("beta provider").await;
    let config = ModuleConfig {
        storage_descriptor: None,
        client: configured_client("alpha", &alpha_url),
        ..config_in(root_dir.path())
    };
    let connection = serve_root(&config).await;

    let active_connection = connection.clone();
    let active_call = tokio::spawn(async move {
        call::<Value>(
            &active_connection,
            OBJECT_PATH,
            "StaticCallTool",
            json!(["alpha", "identity", {}]),
        )
        .await
    });
    // Keep fixture startup separate from the latency assertion below. Windows
    // CI can be heavily loaded while the in-process bus and HTTP server start.
    let started = timeout(Duration::from_secs(15), gate.started.acquire())
        .await
        .expect("the root's old handler enters the provider")
        .unwrap();
    started.forget();

    let response: OpenConfiguredResponse = call_confidential(
        &connection,
        OBJECT_PATH,
        "OpenConfigured",
        json!([configured_open(root_dir.path(), "beta", &beta_url)]),
    )
    .await
    .unwrap();

    // The old root's reader is held at the provider. The transition worker is
    // therefore paused before queuing the object-tree writer. Status must still
    // dispatch through the draining root and return immediately.
    assert_eq!(
        call::<TransitionStatus>(
            &connection,
            OBJECT_PATH,
            "TransitionStatus",
            json!([response.transition_id.clone()]),
        )
        .await
        .unwrap(),
        TransitionStatus::Pending,
    );

    let same_settings_retry: OpenConfiguredResponse = call_confidential(
        &connection,
        OBJECT_PATH,
        "OpenConfigured",
        json!([configured_open(root_dir.path(), "beta", &beta_url)]),
    )
    .await
    .unwrap();
    assert_eq!(same_settings_retry.transition_id, response.transition_id);

    conflicting_open_is_rejected_while_transition_is_pending(
        &connection,
        root_dir.path(),
        &beta_url,
    )
    .await;

    assert_competing_lifecycle_requests_are_rejected(&connection, root_dir.path(), &beta_url).await;

    gate.release.add_permits(1);
    let old_result = timeout(Duration::from_secs(3), active_call)
        .await
        .expect("the old root handler completes")
        .unwrap()
        .unwrap();
    assert_eq!(
        old_result["rendered"]["content"][0]["text"],
        "alpha provider"
    );
    wait_transition(&connection, &response.transition_id).await;
    let names: Vec<String> = call(&connection, OBJECT_PATH, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(names, ["beta"]);
}

async fn assert_competing_lifecycle_requests_are_rejected(
    connection: &Connection,
    data_dir: &std::path::Path,
    endpoint: &str,
) {
    use tokio::time::{Duration, timeout};

    let competing_dir = tempfile::tempdir().unwrap();
    let competing = timeout(
        Duration::from_secs(3),
        call_confidential::<OpenConfiguredResponse>(
            connection,
            OBJECT_PATH,
            "OpenConfigured",
            json!([configured_open(competing_dir.path(), "gamma", endpoint)]),
        ),
    )
    .await
    .expect("a competing lifecycle request does not wait under dispatch")
    .unwrap_err();
    assert_eq!(failure_name(&competing), errors::INVALID_ARGUMENT);
    let close = timeout(
        Duration::from_secs(3),
        call::<Option<String>>(
            connection,
            OBJECT_PATH,
            "Close",
            json!([data_dir.to_str().unwrap()]),
        ),
    )
    .await
    .expect("Close is fenced while replacement owns the lifecycle gate")
    .unwrap_err();
    assert_eq!(failure_name(&close), errors::INVALID_ARGUMENT);
}

#[tokio::test]
async fn closing_waits_for_active_tool_calls_before_acknowledging_cleanup() {
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::time::{Duration, timeout};

    let root_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(ToolCallGate {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let alpha_url = mock_static_server_with_gate("alpha provider", Some(Arc::clone(&gate))).await;
    let connection = serve_root(&config_in(root_dir.path())).await;
    let response = open_configured_complete(
        &connection,
        configured_open(data_dir.path(), "alpha", &alpha_url),
    )
    .await;

    let active_connection = connection.clone();
    let active_path = response.clone();
    let active_call = tokio::spawn(async move {
        call::<Value>(
            &active_connection,
            &active_path,
            "StaticCallTool",
            json!(["alpha", "identity", {}]),
        )
        .await
    });
    let started = timeout(Duration::from_secs(3), gate.started.acquire())
        .await
        .expect("the child call reaches its provider")
        .unwrap();
    started.forget();

    let transition: Option<String> = call(
        &connection,
        OBJECT_PATH,
        "Close",
        json!([data_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    let transition = transition.expect("the child is still active");
    let retry: Option<String> = call(
        &connection,
        OBJECT_PATH,
        "Close",
        json!([data_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert_eq!(retry.as_deref(), Some(transition.as_str()));
    assert_eq!(
        call::<TransitionStatus>(
            &connection,
            OBJECT_PATH,
            "TransitionStatus",
            json!([transition.clone()]),
        )
        .await
        .unwrap(),
        TransitionStatus::Pending,
    );

    gate.release.add_permits(1);
    let result = timeout(Duration::from_secs(3), active_call)
        .await
        .expect("the in-flight tool call completes")
        .unwrap()
        .unwrap();
    assert_eq!(result["rendered"]["content"][0]["text"], "alpha provider");
    wait_transition(&connection, &transition).await;
    let stale_path = call::<Vec<Value>>(&connection, &response, "InstalledList", json!([]))
        .await
        .unwrap_err();
    assert!(matches!(
        stale_path,
        BusError::MethodFailed { ref name, .. }
            if name == "ai.tinyhumans.tinybus.Error.UnknownObject"
    ));
}

fn configured_open(data_dir: &std::path::Path, name: &str, endpoint: &str) -> Value {
    json!({
        "data_dir": data_dir,
        "client": configured_client(name, endpoint),
    })
}

fn configured_client(name: &str, endpoint: &str) -> McpClientConfig {
    McpClientConfig {
        servers: vec![McpServerConfig {
            name: name.into(),
            endpoint: endpoint.into(),
            ..McpServerConfig::default()
        }],
        ..McpClientConfig::default()
    }
}

async fn mock_static_server(reply: &'static str) -> String {
    mock_static_server_with_gate(reply, None).await
}

struct ToolCallGate {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

async fn mock_static_server_with_gate(
    reply: &'static str,
    gate: Option<std::sync::Arc<ToolCallGate>>,
) -> String {
    use axum::Router;
    use axum::routing::post;

    let app = Router::new().route(
        "/",
        post(move |axum::Json(body): axum::Json<Value>| {
            let gate = gate.clone();
            async move {
                let result = match body["method"].as_str().unwrap_or_default() {
                    "initialize" => json!({
                        "protocolVersion": tinymcp_bus::LATEST_PROTOCOL_VERSION,
                        "capabilities": {},
                        "serverInfo": { "name": reply, "version": "1" },
                    }),
                    "tools/list" => json!({ "tools": [{ "name": "identity" }] }),
                    "tools/call" => {
                        if let Some(gate) = gate {
                            gate.started.add_permits(1);
                            let permit = gate.release.acquire().await.unwrap();
                            permit.forget();
                        }
                        json!({
                            "content": [{ "type": "text", "text": reply }],
                            "isError": false,
                        })
                    }
                    _ => json!({}),
                };
                axum::Json(json!({
                    "jsonrpc": "2.0",
                    "id": body["id"].clone(),
                    "result": result,
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{address}/")
}

#[tokio::test]
async fn opening_the_same_directory_twice_returns_the_same_path() {
    let root_dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;
    let arg = json!([other.path().to_str().unwrap()]);

    let first: String = call(&connection, OBJECT_PATH, "Open", arg.clone())
        .await
        .unwrap();
    let second: String = call(&connection, OBJECT_PATH, "Open", arg).await.unwrap();

    assert_eq!(first, second);
}

#[tokio::test]
async fn close_returns_a_transition_that_can_be_awaited_until_the_object_is_removed() {
    let root_dir = tempfile::tempdir().unwrap();
    let child = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;
    let path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([child.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    let transition: String = call(
        &connection,
        OBJECT_PATH,
        "Close",
        json!([child.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    wait_transition(&connection, &transition).await;
    let repeated: Option<String> = call(
        &connection,
        OBJECT_PATH,
        "Close",
        json!([child.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert!(repeated.is_none(), "a completed close is idempotent");
    let unknown: TransitionStatus = call(
        &connection,
        OBJECT_PATH,
        "TransitionStatus",
        json!(["not-a-transition"]),
    )
    .await
    .unwrap();
    assert_eq!(unknown, TransitionStatus::Unknown);
    let error = call::<Vec<Value>>(&connection, &path, "InstalledList", json!([]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            BusError::MethodFailed { ref name, .. }
                if name == "ai.tinyhumans.tinybus.Error.UnknownObject"
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn closing_releases_capacity_and_never_reuses_a_stale_object_path() {
    use tokio::time::{Duration, timeout};

    let root_dir = tempfile::tempdir().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;
    let mut paths = Vec::new();

    for index in 0..(MAX_OPEN_DIRECTORIES + 5) {
        let dir = parent.path().join(index.to_string());
        let path: String = call(
            &connection,
            OBJECT_PATH,
            "Open",
            json!([dir.to_str().unwrap()]),
        )
        .await
        .unwrap();
        let _: Vec<Value> = call(&connection, &path, "InstalledList", json!([]))
            .await
            .unwrap();
        // Legacy Open returns the object path before its background serve
        // transition releases the lifecycle gate. Retry only that documented
        // transient conflict; the bounded wait still catches a stuck open.
        let transition = timeout(Duration::from_secs(5), async {
            loop {
                match call::<Option<String>>(
                    &connection,
                    OBJECT_PATH,
                    "Close",
                    json!([dir.to_str().unwrap()]),
                )
                .await
                {
                    Ok(Some(transition)) => break transition,
                    Ok(None) => panic!("the opened directory is closed"),
                    Err(BusError::MethodFailed { name, message })
                        if name == errors::INVALID_ARGUMENT
                            && message.contains("lifecycle transition is pending") =>
                    {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("could not close the opened directory: {error}"),
                }
            }
        })
        .await
        .expect("the open transition releases its lifecycle gate");
        wait_transition(&connection, &transition).await;
        paths.push(path);
    }

    let mut unique = paths.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        paths.len(),
        "stale object paths are never reused"
    );
}

#[tokio::test]
async fn closing_the_root_resets_its_configuration_and_keeps_the_opener_alive() {
    let root_dir = tempfile::tempdir().unwrap();
    let endpoint = mock_static_server("signed-in provider").await;
    let config = ModuleConfig {
        storage_descriptor: None,
        client: configured_client("signed-in", &endpoint),
        ..config_in(root_dir.path())
    };
    let connection = serve_root(&config).await;

    let before: Vec<String> = call(&connection, OBJECT_PATH, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(before, ["signed-in"]);
    let transition: Option<String> = call(
        &connection,
        OBJECT_PATH,
        "Close",
        json!([root_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    wait_transition(
        &connection,
        transition.as_deref().expect("the root was closed"),
    )
    .await;

    let after: Vec<String> = call(&connection, OBJECT_PATH, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(after, Vec::<String>::new());
    let child_dir = tempfile::tempdir().unwrap();
    let child: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([child_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert_ne!(child, OBJECT_PATH);
    let child_servers: Vec<String> = call(&connection, &child, "StaticList", json!([]))
        .await
        .unwrap();
    assert_eq!(child_servers, Vec::<String>::new());
}

#[tokio::test]
async fn different_directories_get_different_paths() {
    let root_dir = tempfile::tempdir().unwrap();
    let (one, two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let connection = serve_root(&config_in(root_dir.path())).await;

    let first: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([one.path().to_str().unwrap()]),
    )
    .await
    .unwrap();
    let second: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([two.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    assert_ne!(first, second);
}

#[tokio::test]
async fn the_load_time_directory_answers_at_the_root_path() {
    let root_dir = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;

    let path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([root_dir.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    // Opening it again as a second object would put two handles on one file.
    assert_eq!(path, OBJECT_PATH);
}

#[tokio::test]
async fn each_directory_keeps_its_own_records() {
    let root_dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root_dir.path())).await;
    let opened: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([other.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    let _: i64 = call(
        &connection,
        &opened,
        "AuditRecordWrite",
        json!([audit_record("after_login")]),
    )
    .await
    .unwrap();
    let _: i64 = call(
        &connection,
        OBJECT_PATH,
        "AuditRecordWrite",
        json!([audit_record("before_login")]),
    )
    .await
    .unwrap();

    // The whole point of the seam: a call after the switch is answered from the
    // new directory's store, not the one the module was loaded with.
    assert_eq!(audit_tools(&connection, &opened).await, ["after_login"]);
    assert_eq!(
        audit_tools(&connection, OBJECT_PATH).await,
        ["before_login"]
    );
}

#[tokio::test]
async fn a_relative_or_empty_path_is_refused_by_name() {
    let connection = serve_root(&ModuleConfig::default()).await;

    for bad in ["", "relative/dir", "./here"] {
        let error = call::<String>(&connection, OBJECT_PATH, "Open", json!([bad]))
            .await
            .unwrap_err();

        assert_eq!(failure_name(&error), errors::INVALID_ARGUMENT, "{bad:?}");
    }
}

#[tokio::test]
async fn a_refusal_never_echoes_the_path() {
    let connection = serve_root(&ModuleConfig::default()).await;

    let error = call::<String>(
        &connection,
        OBJECT_PATH,
        "Open",
        json!(["relative/secret-user"]),
    )
    .await
    .unwrap_err();

    assert!(!error.to_string().contains("secret-user"), "{error}");
}

#[tokio::test]
async fn an_opened_directory_cannot_open_further_directories() {
    let root_dir = tempfile::tempdir().unwrap();
    let (one, two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let connection = serve_root(&config_in(root_dir.path())).await;
    let opened: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([one.path().to_str().unwrap()]),
    )
    .await
    .unwrap();

    let error = call::<String>(
        &connection,
        &opened,
        "Open",
        json!([two.path().to_str().unwrap()]),
    )
    .await
    .unwrap_err();

    assert_eq!(failure_name(&error), errors::INVALID_ARGUMENT);
}

#[tokio::test]
async fn a_service_that_is_not_a_root_refuses_to_open() {
    let connection = serve_untrusted_root(&ModuleConfig::default()).await;
    // Built the way `Open` builds its objects: no opener.
    connection
        .serve_at(
            "/ai/tinyhumans/tinymcp/Plain".try_into().unwrap(),
            McpService::new(&ModuleConfig::default()).unwrap(),
        )
        .await
        .unwrap();

    let error = call::<String>(
        &connection,
        "/ai/tinyhumans/tinymcp/Plain",
        "Open",
        json!(["/somewhere"]),
    )
    .await
    .unwrap_err();

    assert_eq!(failure_name(&error), errors::INVALID_ARGUMENT);
}

#[tokio::test]
async fn the_number_of_served_directories_is_bounded() {
    let root = tempfile::tempdir().unwrap();
    let connection = serve_root(&config_in(root.path())).await;
    let parent = tempfile::tempdir().unwrap();

    // The root's own directory already counts as one.
    for index in 1..MAX_OPEN_DIRECTORIES {
        let dir = parent.path().join(index.to_string());
        let opened = open_configured_complete(
            &connection,
            json!({
                "data_dir": dir,
                "client": McpClientConfig::default(),
            }),
        )
        .await;
        let _: Vec<Value> = call(&connection, &opened, "InstalledList", json!([]))
            .await
            .unwrap();
    }

    let overflow = parent.path().join("one-too-many");
    let error = call_confidential::<OpenConfiguredResponse>(
        &connection,
        OBJECT_PATH,
        "OpenConfigured",
        json!([{
            "data_dir": overflow,
            "client": McpClientConfig::default(),
        }]),
    )
    .await
    .unwrap_err();
    assert_eq!(failure_name(&error), errors::INVALID_ARGUMENT);
    assert!(!overflow.exists(), "a refused open must create nothing");

    // Directories already served still answer.
    let again: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([parent.path().join("1").to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert!(again.starts_with(DIRECTORY_OBJECT_PREFIX));
}

#[tokio::test]
async fn opener_debug_output_does_not_expose_directory_or_client_data() {
    let connection = serve_root(&ModuleConfig::default()).await;
    let opener = super::DirectoryOpener::new(
        connection.connection.clone(),
        &ModuleConfig::default(),
        SupervisorConfig::default(),
        crate::storage::installed_provider(),
    )
    .unwrap();
    let debug = format!("{opener:?}");
    assert!(debug.contains("DirectoryOpener"));
    assert!(!debug.contains("data_dir"));
}

#[tokio::test]
async fn an_absolute_open_matches_a_relative_load_time_directory() {
    let relative_root = tempfile::tempdir_in(".").unwrap();
    let relative = std::path::Path::new(relative_root.path().file_name().unwrap());
    // Keep the same lexical spelling as load-time normalization. Windows
    // canonicalization adds a verbatim prefix, and Unix may resolve symlinks.
    let absolute = std::env::current_dir().unwrap().join(relative);
    let connection = serve_root(&config_in(relative)).await;
    let path: String = call(
        &connection,
        OBJECT_PATH,
        "Open",
        json!([absolute.to_str().unwrap()]),
    )
    .await
    .unwrap();
    assert_eq!(path, OBJECT_PATH);
}

#[test]
fn serving_failures_keep_the_bus_reason() {
    let error = super::types::serving_error("path already served");
    assert_eq!(
        error.to_string(),
        "bus failure: could not serve the directory: path already served"
    );
}

#[test]
fn configured_directory_paths_are_normalized_to_absolute_paths() {
    let path = super::super::service::absolute_data_dir(std::path::Path::new("relative")).unwrap();
    assert!(path.is_absolute());
}
