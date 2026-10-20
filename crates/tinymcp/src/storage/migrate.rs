//! The v1 to v2 steps: the previous release's `mcp_*` collections and stream
//! become the `mcp.*` ones.
//!
//! Each step is a pure function of the records it is handed, so a resumed or
//! repeated run produces the same output. A step keeps a record's creation
//! time and expiry, renames its `*_at` time fields to `*_ms`, and re-keys it
//! with [`id`]. A cache row or index row that cannot be carried over (its body
//! is not JSON, or the field its new key is built from is missing) is dropped
//! instead of failing the step: it holds nothing the module cannot fetch
//! again, and one bad row must not keep a whole database from migrating.

use serde_json::{Map, Value};
use tinystoragedrivers_core::schema::{SourceRecord, Step, TargetRecord};

#[cfg(feature = "module")]
use tinystoragedrivers_core::schema::delivery::{
    MigrationPayload, MigrationPayloadHandler, MigrationPayloadReply, StreamPlanRequest,
};
#[cfg(feature = "module")]
use tinystoragedrivers_core::schema::{
    StreamMapMode, StreamPlan, TransformError, TransformErrorKind, TransformResult,
};

use super::schema::{
    AUDIT_STREAM, REGISTRY_CACHE, REGISTRY_INDEX, REGISTRY_INDEX_STATE, SERVER_NAMES, SERVERS,
    TOOL_CACHE, doc_id,
};
#[cfg(any(feature = "module", test))]
use super::schema::{
    AUDIT_V2_OPERATION, MIGRATION_TRANSFORM_VERSION, REGISTRY_CACHE_V2_OPERATION,
    REGISTRY_INDEX_STATE_V2_OPERATION, REGISTRY_INDEX_V2_OPERATION, SERVER_NAMES_V2_OPERATION,
    SERVERS_V2_OPERATION, TOOL_CACHE_V2_OPERATION,
};

/// The version the steps read.
const FROM: u32 = 1;
/// The version the steps write.
const TO: u32 = 2;

/// The v1 stream the audit log was appended to.
const LEGACY_AUDIT: &str = "mcp_audit";

/// A document step's per-record rule: the new id and body, or `None` to drop
/// the record.
type Rewrite = fn(
    &SourceRecord,
    Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>>;

/// Every step, in the order they run.
pub(super) fn steps() -> Vec<Step> {
    vec![
        documents("servers_v2", SERVERS, "mcp_servers", server),
        documents("server_names_v2", SERVER_NAMES, "mcp_server_names", claim),
        documents(
            "registry_cache_v2",
            REGISTRY_CACHE,
            "mcp_registry_cache",
            cached_response,
        ),
        documents("tool_cache_v2", TOOL_CACHE, "mcp_tool_cache", cached_tools),
        documents(
            "registry_index_v2",
            REGISTRY_INDEX,
            "mcp_registry_index",
            index_row,
        ),
        documents(
            "registry_index_state_v2",
            REGISTRY_INDEX_STATE,
            "mcp_registry_index_state",
            index_state,
        ),
        Step::stream("audit_v2", AUDIT_STREAM, FROM, TO, |name, entries| {
            Ok(Some((audit_stream_target(&name), entries)))
        })
        .source_prefix(LEGACY_AUDIT),
    ]
}

/// The target-name rule shared by the local audit callback and remote plan.
fn audit_stream_target(name: &str) -> String {
    let rest = name
        .strip_prefix(LEGACY_AUDIT)
        .unwrap_or(name)
        .trim_start_matches('/');
    if rest.is_empty() {
        AUDIT_STREAM.to_owned()
    } else {
        format!("{AUDIT_STREAM}/{rest}")
    }
}

/// A step reading the legacy collection `legacy` and writing `item`, applying
/// `rewrite` to each record's body.
fn documents(id: &'static str, item: &'static str, legacy: &'static str, rewrite: Rewrite) -> Step {
    Step::new(id, item, FROM, TO, move |batch| {
        rewrite_documents(item, batch, rewrite)
    })
    .source(legacy)
}

/// Apply the same document rewrite and metadata rules locally and remotely.
fn rewrite_documents(
    item: &'static str,
    batch: Vec<SourceRecord>,
    rewrite: Rewrite,
) -> tinystoragedrivers_core::Result<Vec<TargetRecord>> {
    let mut targets = Vec::with_capacity(batch.len());
    for record in batch {
        let Value::Object(body) = record.body.clone() else {
            continue;
        };
        if let Some((new_id, body)) = rewrite(&record, body)? {
            let mut target =
                TargetRecord::new(item, new_id, body).expires_ms(record.meta.expires_ms);
            if record.meta.created_ms > 0 {
                target = target.created_ms(record.meta.created_ms);
            }
            targets.push(target);
        }
    }
    Ok(targets)
}

/// Moves `from` to `to` when the record has it.
fn rename(body: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = body.remove(from) {
        body.insert(to.to_owned(), value);
    }
}

/// The string field `name`, if the record has it.
fn text<'a>(body: &'a Map<String, Value>, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}

/// Replaces the JSON text in `from` with the value it holds, under `to`.
/// `None` when the field is missing or is not JSON.
fn unwrap_json(body: &mut Map<String, Value>, from: &str, to: &str) -> Option<()> {
    let parsed = serde_json::from_str::<Value>(text(body, from)?).ok()?;
    body.remove(from);
    body.insert(to.to_owned(), parsed);
    Some(())
}

fn server(
    record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    rename(&mut body, "installed_at", "installed_ms");
    rename(&mut body, "last_connected_at", "last_connected_ms");
    Ok(Some((doc_id(&[&record.id])?, Value::Object(body))))
}

/// A name claim: keyed by the qualified name it holds. A claim from before
/// claims were timed reads as claimed at `0`, i.e. long expired.
fn claim(
    _record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    let Some(name) = text(&body, "qualified_name").map(ToOwned::to_owned) else {
        return Ok(None);
    };
    let claimed = body
        .remove("claimed_at")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    body.insert("claimed_ms".to_owned(), Value::from(claimed));
    Ok(Some((doc_id(&[&name])?, Value::Object(body))))
}

fn cached_response(
    _record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    let Some(key) = text(&body, "cache_key").map(ToOwned::to_owned) else {
        return Ok(None);
    };
    if unwrap_json(&mut body, "body_json", "body").is_none() {
        return Ok(None);
    }
    rename(&mut body, "cached_at", "cached_ms");
    Ok(Some((doc_id(&[&key])?, Value::Object(body))))
}

fn cached_tools(
    _record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    let Some(key) = text(&body, "server_key").map(ToOwned::to_owned) else {
        return Ok(None);
    };
    rename(&mut body, "cached_at", "cached_ms");
    Ok(Some((doc_id(&[&key])?, Value::Object(body))))
}

fn index_row(
    _record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    let (Some(source), Some(name)) = (
        text(&body, "source").map(ToOwned::to_owned),
        text(&body, "qualified_name").map(ToOwned::to_owned),
    ) else {
        return Ok(None);
    };
    if unwrap_json(&mut body, "record_json", "record").is_none() {
        return Ok(None);
    }
    rename(&mut body, "synced_at", "synced_ms");
    Ok(Some((doc_id(&[&source, &name])?, Value::Object(body))))
}

fn index_state(
    _record: &SourceRecord,
    mut body: Map<String, Value>,
) -> tinystoragedrivers_core::Result<Option<(String, Value)>> {
    let Some(source) = text(&body, "source").map(ToOwned::to_owned) else {
        return Ok(None);
    };
    rename(&mut body, "started_at", "started_ms");
    rename(&mut body, "synced_at", "synced_ms");
    Ok(Some((doc_id(&[&source])?, Value::Object(body))))
}

/// Stateless owner-side adapter for the existing `TinyMCP` migration callbacks.
#[cfg(feature = "module")]
pub(crate) struct TinyMcpMigrationHandler;

#[cfg(feature = "module")]
impl MigrationPayloadHandler for TinyMcpMigrationHandler {
    fn transform(&self, payload: MigrationPayload) -> MigrationPayloadReply {
        match payload {
            MigrationPayload::Documents(request) => MigrationPayloadReply::Documents(
                transform_documents(&request.operation, request.version, request.records),
            ),
            MigrationPayload::StreamPlan(request) => {
                MigrationPayloadReply::StreamPlan(transform_stream_plan(&request))
            }
            MigrationPayload::StreamEntries(_) => MigrationPayloadReply::StreamEntries(
                unsupported_operation("TinyMCP audit entries are copied unchanged"),
            ),
            MigrationPayload::BlobTarget(_) => MigrationPayloadReply::BlobTarget(
                unsupported_operation("TinyMCP has no blob migration steps"),
            ),
        }
    }
}

#[cfg(feature = "module")]
fn transform_documents(
    operation: &str,
    version: u32,
    records: Vec<SourceRecord>,
) -> TransformResult<Vec<TargetRecord>> {
    let (item, rewrite) = match operation {
        SERVERS_V2_OPERATION => (SERVERS, server as Rewrite),
        SERVER_NAMES_V2_OPERATION => (SERVER_NAMES, claim as Rewrite),
        REGISTRY_CACHE_V2_OPERATION => (REGISTRY_CACHE, cached_response as Rewrite),
        TOOL_CACHE_V2_OPERATION => (TOOL_CACHE, cached_tools as Rewrite),
        REGISTRY_INDEX_V2_OPERATION => (REGISTRY_INDEX, index_row as Rewrite),
        REGISTRY_INDEX_STATE_V2_OPERATION => (REGISTRY_INDEX_STATE, index_state as Rewrite),
        _ => {
            return unsupported_operation("unknown TinyMCP document migration operation");
        }
    };
    if version != MIGRATION_TRANSFORM_VERSION {
        return Err(transform_error(
            TransformErrorKind::UnsupportedVersion,
            "unsupported TinyMCP document transform version",
        ));
    }
    rewrite_documents(item, records, rewrite)
        .map_err(|_| invalid_migration_input("TinyMCP document transform rejected its input"))
}

#[cfg(feature = "module")]
fn transform_stream_plan(request: &StreamPlanRequest) -> TransformResult<Option<StreamPlan>> {
    if request.operation != AUDIT_V2_OPERATION {
        return unsupported_operation("unknown TinyMCP stream migration operation");
    }
    if request.version != MIGRATION_TRANSFORM_VERSION {
        return Err(transform_error(
            TransformErrorKind::UnsupportedVersion,
            "unsupported TinyMCP stream transform version",
        ));
    }
    if !request.source.starts_with(LEGACY_AUDIT) {
        return Err(invalid_migration_input(
            "TinyMCP audit transform received a source outside its declared prefix",
        ));
    }
    Ok(Some(StreamPlan {
        target: audit_stream_target(&request.source),
        mode: StreamMapMode::Identity,
    }))
}

#[cfg(feature = "module")]
fn unsupported_operation<T>(message: &str) -> TransformResult<T> {
    Err(transform_error(
        TransformErrorKind::UnsupportedOperation,
        message,
    ))
}

#[cfg(feature = "module")]
fn invalid_migration_input(message: &str) -> TransformError {
    transform_error(TransformErrorKind::InvalidInput, message)
}

#[cfg(feature = "module")]
fn transform_error(kind: TransformErrorKind, message: &str) -> TransformError {
    TransformError {
        kind,
        message: message.to_owned(),
    }
}

#[cfg(test)]
#[path = "migrate_tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
