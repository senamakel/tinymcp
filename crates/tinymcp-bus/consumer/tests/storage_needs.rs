//! The standalone consumer sees the canonical TSD types through TinyMCP.

#[test]
fn the_contract_reexports_the_same_exact_and_prefix_need_types() {
    let exact: tinystoragedrivers_bus::StorageNeedsWithExactSecrets<'static> =
        tinymcp_storage_needs_consumer::storage_needs();
    let base: tinystoragedrivers_bus::StorageNeeds<'static> = exact.needs;

    assert_eq!(base.collections.len(), 6);
    assert_eq!(base.secret_prefixes, ["mcp_client_env/"]);
    assert_eq!(exact.exact_secret_names, ["mcp_cache_fingerprint_key"]);
}
