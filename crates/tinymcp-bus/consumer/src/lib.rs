//! An excluded consumer that type-checks TinyMCP's declaration against TSD.

/// Returns the contract declaration for the consumer's type-identity test.
#[must_use]
pub fn storage_needs() -> tinymcp_bus::StorageNeedsWithExactSecrets<'static> {
    tinymcp_bus::storage_needs()
}
