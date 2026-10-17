# Standalone storage-needs consumer

This excluded consumer depends on `tinymcp-bus` and the same canonical
`tinystoragedrivers-bus` v1.5.0 source with default features disabled. Its test
assigns the declaration to the direct TSD types, proving both dependencies
resolve the same Rust types. The standalone lockfile is committed so the
consumer remains reproducible independently of TinyMCP's workspace lockfile.

The consumer's normal dependency graph must not include TinyMCP's
implementation, TinyBus, a storage client, a runtime, or a database driver.
