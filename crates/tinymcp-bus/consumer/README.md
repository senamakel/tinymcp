# Standalone storage-needs consumer

This excluded consumer depends on `tinymcp-bus` and the same canonical
`tinystoragedrivers-bus` v1.8.0 source at commit
`595723e1b1cabeb3d36f0c5b6ff2e11b458e7558` with default features disabled. The
source is released from the `senamakel` fork and resolved through the canonical
repository URL to preserve Cargo package identity. Its test
assigns the declaration to the direct TSD types, proving both dependencies
resolve the same Rust types. The standalone lockfile is committed so the
consumer remains reproducible independently of TinyMCP's workspace lockfile.

The consumer's normal dependency graph must not include TinyMCP's
implementation, TinyBus, a storage client, a runtime, or a database driver.
