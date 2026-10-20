//! The `TinyBus` module: the interface, its state, and the ABI exports.
//!
//! This adapter is the only thing in the crate that knows about `TinyBus`.
//! Everything it serves is the ordinary library API underneath, so the crate
//! stays usable as a plain dependency — which is what lets a host consume it as
//! a path dependency first and a loadable module later without the code in
//! between changing.
//!
//! # The names come from the contract
//!
//! Every member name, the interface, and the object path are spelled once, in
//! [`tinymcp_bus::names`]. The manifest below repeats them because the macro
//! needs literals, and a test asserts the two agree — a member served but not
//! declared is invisible to a host, and one declared but not served is an
//! unknown-method failure at the worst possible moment.
//!
//! # Arguments are positional
//!
//! `TinyBus` decodes a member's arguments from a JSON array by position. That
//! makes the *order* of a method's parameters part of the contract, not just
//! their types: swapping two parameters of the same type is a silent breaking
//! change. Each signature below matches the order documented on its member.

mod config;
mod directories;
mod maintenance;
#[allow(clippy::unused_async)]
mod service;

pub use config::ModuleConfig;
pub use service::McpService;

use std::sync::Arc;
use tinybus::{Connection, Result as TinyBusResult};
use tinybus_module::ModuleSetupContext;
use tinymcp_bus::names;

use crate::registry::SupervisorConfig;
use crate::storage::{TinyMcpMigrationHandler, storage_schema_declaration};

/// Builds the service and serves it.
///
/// A failure here fails the load. That is deliberate: a module that came up
/// without its store or without a working HTTP client would answer every call
/// with the same error, and failing at load says so once rather than on every
/// request afterwards.
///
/// Connecting is the opposite: the boot pass and the reconnect supervisor start
/// here as background work and `setup` does not wait for them, so a server that
/// is down delays nothing. See [`maintenance`].
pub(super) async fn setup(connection: Connection, config: ModuleConfig) -> TinyBusResult<()> {
    if let Some(descriptor) = config.storage_descriptor.clone() {
        let provider = crate::storage::BusProvider::connect_bound(connection.clone(), descriptor)
            .await
            .map_err(|error| tinybus::Error::failed(format!("tinymcp could not start: {error}")))?;
        if config.data_dir.is_none() {
            return Err(tinybus::Error::failed(
                "tinymcp host storage descriptor requires a data directory",
            ));
        }
        return setup_with_provider(connection, config, Arc::new(provider)).await;
    }

    // A module that keeps state reaches the host's storage over the bus; the
    // host binds the scope and the grant. A host that linked the module into
    // its own process and installed a provider has already chosen. Failing
    // here fails the load, like any other missing dependency: falling back to
    // memory would quietly lose every install at the next restart.
    //
    // Directory operations (`Open`, `OpenConfigured`, `HasStoredState`) build
    // services over the installed provider whatever the load-time `data_dir`
    // was, so a host with no root directory still needs host storage. There the
    // storage is best-effort: a root with nothing to persist keeps working
    // without it, and the failure is logged rather than failing the load.
    let provider = if crate::storage::provider_installed() {
        None
    } else {
        host_storage_for(connection.clone(), &config).await?
    };
    if let Some(provider) = provider {
        crate::storage::install_provider(provider);
    }
    setup_over_installed_storage(connection, config).await
}

/// Registers module-owned migration transforms before ordinary storage setup.
pub(super) async fn setup_with_context(
    context: ModuleSetupContext,
    config: ModuleConfig,
) -> TinyBusResult<()> {
    let declaration = storage_schema_declaration()
        .map_err(|_| tinybus::Error::failed("TinyMCP migration schema unavailable"))?;
    tinystoragedrivers_tinybus::migration::serve_module_migration(
        &context,
        declaration,
        Arc::new(TinyMcpMigrationHandler),
    )
    .await?;
    setup(context.connection().clone(), config).await
}

/// The host-storage provider to install for `config`, if one can be had.
///
/// `None` means the host has no storage to give a root that persists nothing,
/// so directory operations stay on the in-memory default.
pub(super) async fn host_storage_for(
    connection: Connection,
    config: &ModuleConfig,
) -> TinyBusResult<Option<Arc<dyn crate::storage::StorageProvider>>> {
    match bus_provider(connection).await {
        Ok(provider) => Ok(Some(provider)),
        Err(error) if config.data_dir.is_none() => {
            tracing::warn!(
                %error,
                "tinymcp: host storage unavailable; directory operations stay in memory"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// The provider that reaches the host's storage over `connection`.
pub(super) async fn bus_provider(
    connection: Connection,
) -> TinyBusResult<Arc<dyn crate::storage::StorageProvider>> {
    let provider = crate::storage::BusProvider::connect(connection)
        .await
        .map_err(|error| {
            tinybus::Error::failed(format!("tinymcp could not reach host storage: {error}"))
        })?;
    Ok(Arc::new(provider))
}

/// How `client`'s stdio servers start, given the bus `connection` to `TinyBox`.
///
/// A configured sandbox becomes a jailed launcher over `connection`. With no
/// sandbox configured, or none reachable, the result starts only
/// operator-trusted servers: the missing jail is a refusal, never a fallback.
pub(super) fn stdio_launchers(
    connection: Option<&Connection>,
    client: &tinymcp_bus::McpClientConfig,
) -> crate::transport::stdio::jailed::StdioLaunchers {
    use crate::transport::stdio::jailed::{BusJail, JailedSpawner, StdioLaunchers};
    let Some(sandbox) = client.stdio_sandbox.clone() else {
        return StdioLaunchers::new();
    };
    if let Some(Ok(bus)) = connection.map(BusJail::connect) {
        return StdioLaunchers::sandboxed(Arc::new(JailedSpawner::new(Arc::new(bus), sandbox)));
    }
    tracing::warn!("a stdio sandbox is configured but the sandbox module is not reachable");
    StdioLaunchers::new()
}

/// [`setup`] over whatever storage provider is installed already.
pub(super) async fn setup_over_installed_storage(
    connection: Connection,
    config: ModuleConfig,
) -> TinyBusResult<()> {
    setup_with_provider(connection, config, crate::storage::installed_provider()).await
}

async fn setup_with_provider(
    connection: Connection,
    config: ModuleConfig,
    provider: Arc<dyn crate::storage::StorageProvider>,
) -> TinyBusResult<()> {
    let supervisor = SupervisorConfig::default();
    let service = Arc::new(
        McpService::with_provider_and_stdio(
            &config,
            provider.clone(),
            stdio_launchers(Some(&connection), &config.client),
        )
        .map_err(|error| tinybus::Error::failed(format!("tinymcp could not start: {error}")))?
        .with_maintenance(supervisor.clone())
        .await
        .with_opener(connection.clone(), &config, supervisor, provider)
        .map_err(|error| tinybus::Error::failed(format!("tinymcp could not start: {error}")))?,
    );

    service.register_root();

    connection
        .serve_at(names::OBJECT_PATH.try_into()?, service.dispatch_interface())
        .await?;
    connection.request_name(names::INTERFACE).await?;

    Ok(())
}

macro_rules! export_module {
    ($($declaration:tt)*) => {
        #[cfg(any(feature = "static-link", test))]
        mod linked_exports {
            // TinyBus generates these three ABI items without rustdoc.
            #![cfg_attr(not(test), expect(missing_docs, reason = "generated TinyBus ABI entries"))]
            use super::*;
            tinybus_module::module_export_static! { $($declaration)* }
        }
        #[cfg(all(feature = "static-link", not(test)))]
        pub use linked_exports::{
            TINYBUS_MODULE_ABI_V1, linked_module, tinybus_module_init_v1,
            tinybus_module_manifest_v1,
        };
        #[cfg(test)]
        pub(crate) use linked_exports::linked_module as test_linked_module;
        #[cfg(not(any(feature = "static-link", test)))]
        mod dynamic_exports {
            use super::*;
            tinybus_module::module_export! { $($declaration)* }
        }
    };
}

export_module! {
    setup_with_context = setup_with_context,
    config = ModuleConfig,
    // More than one, because a tool call on one server must not wait behind a
    // slow call on another: these are third-party endpoints and subprocesses,
    // and one of them being slow is routine.
    worker_threads = 4,
    provides = ["ai.tinyhumans.tinymcp.Mcp", "org.tinystoragedrivers.Migration"],
    methods = [
        "RegistrySearch",
        "RegistrySearchCurated",
        "RegistryGet",
        "RegistrySettingsGet",
        "RegistrySettingsSet",
        "ConfigDocumentGet",
        "ConfigDocumentApply",
        "HasStoredState",
        "InstalledList",
        "Install",
        "Uninstall",
        "SetEnabled",
        "UpdateEnv",
        "Connect",
        "Disconnect",
        "Status",
        "ConnectedOverview",
        "CachedOverview",
        "DetectAuth",
        "OAuthBegin",
        "OAuthComplete",
        "ListTools",
        "ToolCall",
        "ReadResource",
        "ConfigAssist",
        "SetupSearch",
        "SetupGet",
        "SetupRequestSecret",
        "SetupSubmitSecret",
        "SetupTestConnection",
        "SetupInstallAndConnect",
        "StaticList",
        "StaticServerSummaries",
        "ConfiguredToolCall",
        "ValidateServerArguments",
        "StaticListTools",
        "StaticScrubTools",
        "StaticCachedListTools",
        "StaticCallTool",
        "StaticReadResource",
        "PrepareBridgeTools",
        "StaticCallToolClassified",
        "AuditRecordWrite",
        "AuditListWrites",
        "DrainSupervisorEvents",
        "ServerOpen",
        "ServerOpenHttp",
        "ServerSubmit",
        "ServerPoll",
        "ServerComplete",
        "ServerCancel",
        "ServerClose",
        "ServerShutdown",
        "ServerSessionMetadata",
        "ServerHttpSession",
        "TransformText",
        "NormalizeToolArguments",
        "DisplayRemoteTool",
        "RenderToolOutput",
        "PrepareAgentTools",
        "Open",
        "OpenConfigured",
        "Close",
        "TransitionStatus",
    ],
    signals = [],
    requires = [],
    // The sandbox module, used only when the host configures a stdio sandbox.
    optional = ["ai.tinyhumans.tinybox.Box"],
    // Not lazy: a host that loaded this module wants its servers connected, and
    // deferring the load would defer that until the first call — by which point
    // an agent has already been told it has no tools. `setup` starts the boot
    // connect pass and the supervisor for exactly that reason.
    lazy = false,
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
