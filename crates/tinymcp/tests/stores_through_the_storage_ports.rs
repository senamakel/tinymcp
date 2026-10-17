//! The public stores, driven only through the storage ports.
//!
//! The module links no database: a host hands it ports, and what one store
//! writes, a store reopened over the same ports reads back. These tests use the
//! in-memory backend and never touch the filesystem.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use tinymcp::storage::{self, StoragePorts};
use tinymcp::{AuditStore, CommandKind, InstalledServer, NewMcpWriteRecord, Store, Transport};

fn server(id: &str, name: &str) -> InstalledServer {
    InstalledServer {
        server_id: id.to_string(),
        qualified_name: name.to_string(),
        display_name: "Weather".to_string(),
        description: Some("Forecasts".to_string()),
        icon_url: None,
        command_kind: CommandKind::Node,
        command: "weather-mcp".to_string(),
        args: vec!["--verbose".to_string()],
        env_keys: vec!["API_KEY".to_string()],
        config: None,
        installed_at: 1_700_000_000,
        last_connected_at: Some(1_700_000_100),
        transport: Transport::Stdio,
        enabled: true,
    }
}

#[test]
fn installs_and_credentials_survive_reopening_over_the_same_ports() {
    let ports = StoragePorts::memory().unwrap();
    let store = Store::open(ports.clone()).unwrap();
    store
        .insert_server(&server("srv-1", "@acme/weather"))
        .unwrap();
    store
        .set_env_values(
            "srv-1",
            &BTreeMap::from([("API_KEY".to_string(), "sekrit".to_string())]),
        )
        .unwrap();
    drop(store);

    let reopened = Store::open(ports).unwrap();
    let loaded = reopened.get_server("srv-1").unwrap();
    assert_eq!(loaded, server("srv-1", "@acme/weather"));
    assert_eq!(
        reopened.load_env_values("srv-1").unwrap().get("API_KEY"),
        Some(&"sekrit".to_string())
    );
}

#[test]
fn the_audit_log_and_the_servers_share_ports_but_not_records() {
    let ports = StoragePorts::memory().unwrap();
    let store = Store::open(ports.clone()).unwrap();
    let audit = AuditStore::open(ports).unwrap();
    store
        .insert_server(&server("srv-1", "@acme/weather"))
        .unwrap();
    audit
        .record(&NewMcpWriteRecord {
            timestamp_ms: 10,
            client_info: "claude".into(),
            tool_name: "memory_write".into(),
            args_summary: serde_json::json!({}),
            resulting_chunk_id: None,
            success: true,
            error_message: None,
        })
        .unwrap();

    assert_eq!(store.list_servers().unwrap().len(), 1);
    assert_eq!(
        audit
            .list(&tinymcp::McpWriteListQuery::default())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn the_bus_contract_declares_every_persistent_need_with_its_indexes() {
    use tinymcp_bus::StorageNeeds;

    let needs: StorageNeeds<'_> = tinymcp_bus::storage_needs();
    let names: Vec<_> = needs
        .collections
        .iter()
        .map(|spec| spec.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "mcp_servers",
            "mcp_server_names",
            "mcp_registry_cache",
            "mcp_tool_cache",
            "mcp_registry_index",
            "mcp_registry_index_state",
        ]
    );

    let indexes = |collection: &str| {
        needs
            .collections
            .iter()
            .find(|spec| spec.name == collection)
            .unwrap()
            .indexes
            .iter()
            .map(|index| {
                (
                    index.name.as_str(),
                    index.fields.iter().map(String::as_str).collect::<Vec<_>>(),
                    index.unique,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        indexes("mcp_servers"),
        [
            ("qualified_name", vec!["qualified_name"], false),
            ("installed_at", vec!["installed_at"], false),
        ]
    );
    assert_eq!(
        indexes("mcp_registry_cache"),
        [("cache_key", vec!["cache_key"], false)]
    );
    assert_eq!(
        indexes("mcp_registry_index"),
        [("source", vec!["source"], false)]
    );
    assert!(needs.collections.iter().all(|spec| spec.search.is_none()));
    assert!(needs.blob_prefixes.is_empty());
    assert_eq!(needs.stream_prefixes, ["mcp_audit"]);
    assert_eq!(
        needs.secret_prefixes,
        ["mcp_client_env/", "mcp_cache_fingerprint_key"]
    );
}
