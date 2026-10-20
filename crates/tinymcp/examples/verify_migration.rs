//! Exercises TinyMCP's owner migration contract through its compiled module.
//!
//! The fixture uses only synthetic `MemoryStorage` data and in-process TinyBus
//! brokers. It migrates before the host serves ordinary storage to the module.

use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::transport::memory::MemoryTransport;
use tinybus::{BusName, Connection, broker::Broker, module::ModuleHost};
use tinymcp::{InstalledServer, names};
use tinystoragedrivers_busproxy::{
    AuditEvent, AuditSink, Environment, GovernedEndpointLimits, GovernedStorage, GrantPolicy,
    HostContext, HostFailure, HostStorageContext, ResolvedStorage, StorageGrant,
};
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
    storage_host: Connection,
    module_host: ModuleHost,
    module_name: String,
    module_peer: BusName,
    migration: Option<MigrationModuleClient>,
}

#[tokio::main]
async fn main() -> VerifyResult<()> {
    let source = module_argument()?;
    let (_unselected_staging, unselected_module) = stage_allowlisted_module(&source)?;
    verify_unselected_setup_fails(&unselected_module).await?;

    let mut fixture = load_selected_module(&source).await?;
    verify_migration_authorization(&fixture).await?;
    let migration = fixture
        .migration
        .take()
        .ok_or_else(|| std::io::Error::other("migration client was already consumed"))?;
    let backend = migrate_legacy_records(migration).await?;
    verify_normal_storage_after_migration(&fixture, backend).await?;

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
    let storage_host = connect_peer(&broker, false).await?;
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
        storage_host,
        module_name: info.name,
        module_host,
        module_peer,
        migration: Some(migration),
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
        "command": "fixture-disabled-server",
        "args": [],
        "env_keys": ["TOKEN"],
        "installed_at": 1_700_000_000_000_i64,
        "last_connected_at": 1_700_000_100_000_i64,
        "transport": "stdio",
        "enabled": false,
        "catalog_managed": true,
    })
}

#[derive(Clone, Copy)]
struct StorageProfile(&'static str);

impl HostContext for StorageProfile {}

struct MemoryStorageEnvironment {
    backend: Arc<MemoryStorage>,
    secrets: Arc<MemorySecrets>,
}

#[async_trait]
impl Environment<StorageProfile> for MemoryStorageEnvironment {
    async fn resolve(
        &self,
        context: &StorageProfile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        let scope = Scope::new(context.0).map_err(|_| HostFailure::Denied("invalid-test-scope"))?;
        let storage = self
            .backend
            .for_scope(&scope)
            .map_err(|_| HostFailure::Denied("test-storage-unavailable"))?;
        let grant = StorageGrant::from_needs_with_exact_secrets(
            tinymcp_bus::storage_needs(),
            &GrantPolicy {
                writable: true,
                allow_secrets: true,
                ..GrantPolicy::default()
            },
        )
        .map_err(|_| HostFailure::Denied("invalid-test-grant"))?;
        Ok(ResolvedStorage {
            backend_identity: "tinymcp-migration-fixture-memory".to_owned(),
            binding_epoch: 1,
            canonical_scope: context.0.to_owned(),
            grant,
            server: Arc::new(StorageServer::new(storage).with_secrets(self.secrets.clone())),
        })
    }

    async fn admit_write(
        &self,
        _context: &StorageProfile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<(), HostFailure> {
        Ok(())
    }
}

#[derive(Default)]
struct NoAudit;

impl AuditSink for NoAudit {
    fn record(&self, _event: AuditEvent) {}
}

async fn migrate_legacy_records(
    migration: MigrationModuleClient,
) -> VerifyResult<Arc<MemoryStorage>> {
    let backend = Arc::new(MemoryStorage::new());
    let declaration = migration.declaration().clone();
    let registry =
        Registry::compose_with_declarations(std::iter::empty(), std::iter::once(declaration))?;
    registry.prepare(backend.as_ref()).await?;
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
    Ok(backend)
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
    assert_eq!(migrated.doc["enabled"], false);
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
    fixture: &SelectedModule,
    backend: Arc<MemoryStorage>,
) -> VerifyResult<()> {
    let secrets = Arc::new(MemorySecrets::new());
    let endpoint = GovernedStorage::new(
        Arc::new(MemoryStorageEnvironment { backend, secrets }),
        Arc::new(NoAudit),
        GovernedEndpointLimits::default(),
    );
    let registration = tinystoragedrivers_tinybus::attach_storage(
        &fixture.module_host,
        &fixture.storage_host,
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .map_err(|_| std::io::Error::other("governed storage service registration failed"))?;
    let (_lease, descriptor) = registration
        .register_lease(
            HostStorageContext::new(StorageProfile("module-tinymcp")),
            fixture.module_peer.clone(),
        )
        .await
        .map_err(|_| std::io::Error::other("ordinary storage lease registration failed"))?;
    let before_owner = fixture.controller.name_owner(names::INTERFACE).await?;
    assert_eq!(before_owner.as_ref(), Some(&fixture.module_peer));
    let data_dir = tempfile::tempdir()?;
    fixture
        .module_host
        .reinitialize(
            &fixture.module_name,
            json!({
                "data_dir": data_dir.path().to_string_lossy(),
                "storage_descriptor": descriptor,
            }),
        )
        .await?;
    let after_owner = fixture.controller.name_owner(names::INTERFACE).await?;
    assert_eq!(after_owner.as_ref(), Some(&fixture.module_peer));
    let migration =
        MigrationModuleClient::discover(&fixture.controller, &fixture.module_peer).await?;
    assert_eq!(migration.declaration().owner.as_str(), "mcp");
    let proxy = fixture.controller.proxy(
        fixture.module_peer.as_str(),
        names::OBJECT_PATH,
        names::INTERFACE,
    )?;
    let installed: Vec<InstalledServer> = proxy.call(names::methods::INSTALLED_LIST, ()).await?;
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].server_id, "srv-1");
    assert!(
        !installed[0].enabled,
        "the disabled fixture remains disabled"
    );
    let _: tinymcp_bus::RegistrySettings = proxy
        .call(names::methods::REGISTRY_SETTINGS_GET, ())
        .await?;

    let child_dir = tempfile::tempdir()?;
    let child_path: String = proxy
        .call_confidential(
            names::methods::OPEN,
            (child_dir.path().to_string_lossy().to_string(),),
        )
        .await?;
    let child = fixture.controller.proxy(
        fixture.module_peer.as_str(),
        child_path.as_str(),
        names::INTERFACE,
    )?;
    let child_installed: Vec<InstalledServer> =
        child.call(names::methods::INSTALLED_LIST, ()).await?;
    assert_eq!(child_installed.len(), 1);
    assert_eq!(child_installed[0].server_id, "srv-1");

    let malformed_descriptor = json!({
        "service_peer": "not-a-unique-peer",
        "registration_id": 77,
        "object_root": "/ai/tinyhumans/tinystorage/Leases/77"
    });
    assert!(
        fixture
            .module_host
            .reinitialize(
                &fixture.module_name,
                json!({
                    "data_dir": data_dir.path().to_string_lossy(),
                    "storage_descriptor": malformed_descriptor,
                }),
            )
            .await
            .is_err(),
        "a malformed host route must fail setup without replacing the running service"
    );
    assert!(
        !fixture.module_host.list().iter().any(|module| {
            module.name == fixture.module_name
                && matches!(module.state, tinybus::module::ModuleState::Faulted { .. })
        }),
        "a rejected storage descriptor must leave the admitted module live"
    );
    let after_malformed: Vec<InstalledServer> =
        proxy.call(names::methods::INSTALLED_LIST, ()).await?;
    assert_eq!(after_malformed.len(), 1);
    assert_eq!(after_malformed[0].server_id, "srv-1");

    let (revoked_lease, revoked_descriptor) = registration
        .register_lease(
            HostStorageContext::new(StorageProfile("revoked-route")),
            fixture.module_peer.clone(),
        )
        .await
        .map_err(|_| std::io::Error::other("revoked-route lease registration failed"))?;
    assert!(registration.revoke(&revoked_lease).await);
    assert!(
        fixture
            .module_host
            .reinitialize(
                &fixture.module_name,
                json!({
                    "data_dir": data_dir.path().to_string_lossy(),
                    "storage_descriptor": revoked_descriptor,
                }),
            )
            .await
            .is_err(),
        "a revoked host route must fail setup without replacing the running service"
    );
    let after_revoked: Vec<InstalledServer> =
        proxy.call(names::methods::INSTALLED_LIST, ()).await?;
    assert_eq!(after_revoked.len(), 1);
    assert_eq!(after_revoked[0].server_id, "srv-1");
    Ok(())
}
