//! Exercises TinyMCP's owner migration contract through its compiled module.
//!
//! The fixture uses only synthetic `MemoryStorage` data and in-process TinyBus
//! brokers. It migrates before the host serves ordinary storage to the module.

use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde_json::{Value, json};
use tinybus::transport::memory::MemoryTransport;
use tinybus::{BusName, Connection, broker::Broker, module::ModuleHost};
use tinymcp::{InstalledServer, names};
use tinystoragedrivers_busproxy::{PerCallerEndpoint, serve};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{
    CollectionSpec, MemoryStorage, Precondition, Scope, ScopedStorage, StorageBackend,
    WriteOptions,
    schema::{MigrateOptions, Registry, delivery::MigrationDeliveryClient},
};
use tinystoragedrivers_secrets::MemorySecrets;
use tinystoragedrivers_tinybus::migration::{
    EXCHANGE_MEMBER, MIGRATION_INTERFACE, MIGRATION_OBJECT_PATH, MigrationModuleClient,
};

type VerifyResult<T> = Result<T, Box<dyn Error>>;

struct SelectedModule {
    _staging: tempfile::TempDir,
    controller: Connection,
    outsider: Connection,
    _module_host: ModuleHost,
    module_peer: BusName,
    migration: MigrationModuleClient,
}

#[tokio::main]
async fn main() -> VerifyResult<()> {
    let source = module_argument()?;
    let (_unselected_staging, unselected_module) = stage_allowlisted_module(&source)?;
    verify_unselected_setup_fails(&unselected_module).await?;

    let fixture = load_selected_module(&source).await?;
    verify_migration_authorization(&fixture).await?;
    let storage = migrate_legacy_records(fixture.migration).await?;
    verify_normal_storage_after_migration(&source, storage).await?;

    println!("verified TinyMCP owner transforms through a compiled module");
    Ok(())
}

fn module_argument() -> Result<PathBuf, std::io::Error> {
    std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "usage: cargo run -p tinymcp --example verify_migration -- <module-path>",
            )
        })
}

fn stage_allowlisted_module(source: &Path) -> VerifyResult<(tempfile::TempDir, PathBuf)> {
    let parent = source
        .parent()
        .ok_or_else(|| std::io::Error::other("module artifact has no parent directory"))?;
    let filename = source
        .file_name()
        .ok_or_else(|| std::io::Error::other("module artifact has no filename"))?;
    let staging = tempfile::tempdir_in(parent)?;
    let staged = staging.path().join(filename);
    std::fs::copy(source, &staged)?;
    let digest = tinybus::module::sha256_file(&staged)?;
    std::fs::write(
        staging.path().join("modules.toml"),
        format!("{:?} = {:?}\n", filename.to_string_lossy(), digest),
    )?;
    Ok((staging, staged))
}

async fn verify_unselected_setup_fails(module: &Path) -> VerifyResult<()> {
    let modules = ModuleHost::new(Broker::new());
    let info = modules.load_file(module)?;
    assert!(
        modules
            .ensure_ready(info.manifest.bus_name.as_str())
            .await
            .is_err(),
        "the migration-required module fails setup without a selected host binding"
    );
    Ok(())
}

async fn connect_peer(broker: &Broker, selected: bool) -> tinybus::Result<Connection> {
    let (broker_transport, client_transport) = MemoryTransport::pair();
    if selected {
        broker.attach_module_controller(Arc::new(broker_transport))?;
    } else {
        broker.attach(Arc::new(broker_transport));
    }
    Connection::connect(Box::new(client_transport)).await
}

async fn load_selected_module(source: &Path) -> VerifyResult<SelectedModule> {
    let (staging, staged) = stage_allowlisted_module(source)?;
    let broker = Broker::new_restricted_module_control();
    let controller = connect_peer(&broker, true).await?;
    let outsider = connect_peer(&broker, false).await?;
    let module_host = ModuleHost::new(broker);
    let info = module_host.load_file(&staged)?;
    if let Err(error) = module_host
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
    {
        let state = module_host
            .list()
            .into_iter()
            .find(|module| module.manifest.bus_name == info.manifest.bus_name)
            .map(|module| module.state);
        return Err(std::io::Error::other(format!(
            "selected-controller TinyMCP setup failed: {error}; state: {state:?}"
        ))
        .into());
    }
    let module_peer = controller
        .name_owner(names::INTERFACE)
        .await?
        .ok_or_else(|| {
            std::io::Error::other("compiled TinyMCP module did not claim its bus name")
        })?;
    let migration = MigrationModuleClient::discover(&controller, &module_peer).await?;
    assert_eq!(migration.declaration().owner.as_str(), "mcp");
    Ok(SelectedModule {
        _staging: staging,
        controller,
        outsider,
        _module_host: module_host,
        module_peer,
        migration,
    })
}

async fn verify_migration_authorization(fixture: &SelectedModule) -> VerifyResult<()> {
    let outsider = fixture.outsider.proxy(
        fixture.module_peer.as_str(),
        MIGRATION_OBJECT_PATH,
        MIGRATION_INTERFACE,
    )?;
    let denied = outsider
        .call_confidential::<Value>(EXCHANGE_MEMBER, json!(17))
        .await
        .expect_err("malformed outsider input must be denied before decoding");
    assert!(
        denied
            .to_string()
            .contains("migration authority unavailable")
    );

    let selected = fixture.controller.proxy(
        fixture.module_peer.as_str(),
        MIGRATION_OBJECT_PATH,
        MIGRATION_INTERFACE,
    )?;
    assert!(
        selected.call::<Value>(EXCHANGE_MEMBER, ()).await.is_err(),
        "migration calls require confidential transport"
    );
    Ok(())
}

fn legacy_server() -> Value {
    json!({
        "server_id": "srv-1",
        "qualified_name": "@acme/files",
        "display_name": "Files",
        "command_kind": "node",
        "command": "npx",
        "args": ["-y", "files"],
        "env_keys": ["TOKEN"],
        "installed_at": 1_700_000_000_000_i64,
        "last_connected_at": 1_700_000_100_000_i64,
        "transport": "stdio",
        "enabled": true,
        "catalog_managed": true,
    })
}

async fn migrate_legacy_records(migration: MigrationModuleClient) -> VerifyResult<ScopedStorage> {
    let backend = MemoryStorage::new();
    let declaration = migration.declaration().clone();
    let registry =
        Registry::compose_with_declarations(std::iter::empty(), std::iter::once(declaration))?;
    registry.prepare(&backend).await?;
    let scope = Scope::new("module-tinymcp")?;
    let storage = backend.for_scope(&scope)?;
    storage
        .documents()
        .ensure_collection(&CollectionSpec::new("mcp_servers"))
        .await?;
    let expiry = 4_100_000_000_000_i64;
    storage
        .documents()
        .put_with(
            "mcp_servers",
            "srv-1",
            legacy_server(),
            Precondition::Absent,
            WriteOptions::new().schema(1).expires_ms(expiry),
        )
        .await?;
    let source = storage
        .documents()
        .get("mcp_servers", "srv-1")
        .await?
        .ok_or_else(|| std::io::Error::other("legacy server record was not seeded"))?;
    let audit = vec![
        json!({"timestamp_ms":1, "tool_name":"first", "success":true}),
        json!({"timestamp_ms":2, "tool_name":"second", "success":false}),
    ];
    for entry in &audit {
        storage.streams().append("mcp_audit", entry.clone()).await?;
    }

    let executor = MigrationDeliveryClient::new(Arc::new(migration));
    registry
        .migrate_with_executor(&storage, &MigrateOptions::new(), &executor)
        .await?;
    registry.check_ready_in(&scope, "mcp.servers")?;
    registry.check_ready_in(&scope, "mcp.audit")?;
    assert_migrated_server(&storage, &source.meta, expiry).await?;
    assert_migrated_audit_stream(&storage, &audit).await?;
    Ok(storage)
}

async fn assert_migrated_server(
    storage: &ScopedStorage,
    source_meta: &tinystoragedrivers_core::Meta,
    expiry: i64,
) -> VerifyResult<()> {
    let migrated = storage
        .documents()
        .get("mcp.servers", "srv-1")
        .await?
        .ok_or_else(|| std::io::Error::other("server row did not migrate"))?;
    assert_eq!(migrated.doc["installed_ms"], 1_700_000_000_000_i64);
    assert_eq!(migrated.doc["last_connected_ms"], 1_700_000_100_000_i64);
    assert_eq!(migrated.meta.created_ms, source_meta.created_ms);
    assert_eq!(migrated.meta.expires_ms, Some(expiry));
    assert!(
        storage
            .documents()
            .get("mcp_servers", "srv-1")
            .await?
            .is_none(),
        "the legacy server source must be deleted after migration"
    );
    Ok(())
}

async fn assert_migrated_audit_stream(
    storage: &ScopedStorage,
    expected: &[Value],
) -> VerifyResult<()> {
    let length = storage.streams().len("mcp.audit").await?;
    let entries = storage.streams().read_window("mcp.audit", 0, 16).await?;
    assert_eq!(length, expected.len() as u64);
    assert_eq!(
        entries
            .into_iter()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(storage.streams().len("mcp_audit").await?, 0);
    Ok(())
}

async fn verify_normal_storage_after_migration(
    source: &Path,
    storage: ScopedStorage,
) -> VerifyResult<()> {
    let broker = Broker::new_restricted_module_control();
    let host_connection = connect_peer(&broker, false).await?;
    let storage_for_peer = storage.clone();
    let secrets = Arc::new(MemorySecrets::new());
    let endpoint = PerCallerEndpoint::new(move |_sender| {
        Some(StorageServer::new(storage_for_peer.clone()).with_secrets(secrets.clone()))
    });
    let _served = serve(&host_connection, Arc::new(endpoint)).await?;
    let controller = connect_peer(&broker, true).await?;
    let (_staging, staged) = stage_allowlisted_module(source)?;
    let modules = ModuleHost::new(broker);
    let data_dir = tempfile::tempdir()?;
    let info = modules.load_file_with_config(
        &staged,
        json!({"data_dir":data_dir.path().to_string_lossy()}),
    )?;
    if let Err(error) = modules
        .ensure_ready(info.manifest.bus_name.as_str())
        .await
    {
        let state = modules
            .list()
            .into_iter()
            .find(|module| module.manifest.bus_name == info.manifest.bus_name)
            .map(|module| module.state);
        return Err(std::io::Error::other(format!(
            "storage-bound TinyMCP setup failed: {error}; state: {state:?}"
        ))
        .into());
    }
    let module_peer = controller
        .name_owner(names::INTERFACE)
        .await?
        .ok_or_else(|| std::io::Error::other("storage-bound TinyMCP did not claim its bus name"))?;
    let proxy = controller.proxy(module_peer.as_str(), names::OBJECT_PATH, names::INTERFACE)?;
    let installed: Vec<InstalledServer> = proxy.call(names::methods::INSTALLED_LIST, ()).await?;
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].server_id, "srv-1");
    let _: tinymcp_bus::RegistrySettings = proxy
        .call(names::methods::REGISTRY_SETTINGS_GET, ())
        .await?;
    Ok(())
}
