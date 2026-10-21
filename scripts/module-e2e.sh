#!/usr/bin/env bash
set -euo pipefail

# Build the real cdylib and exercise it through TinyBus's native module loader.
cargo build --package tinymcp --lib

case "${RUNNER_OS:-$(uname -s)}" in
  Windows|MINGW*|MSYS*|CYGWIN*)
    # The hosted Windows workspace inherits an ACL that lets another account
    # write under target/debug. Preserve TinyBus's loader admission check by
    # copying only the test artifact to a private, trusted-ACL directory under
    # target/ before loading it.
    powershell.exe -NoProfile -NonInteractive -File scripts/prepare-module-e2e.ps1
    module_path="target/module-e2e-private/tinymcp.dll"
    ;;
  macOS|Darwin) module_path="target/debug/libtinymcp.dylib" ;;
  *) module_path="target/debug/libtinymcp.so" ;;
esac

if [[ ! -f "$module_path" ]]; then
  echo "expected built TinyMCP module at $module_path" >&2
  exit 1
fi

cargo run --package tinymcp --example verify_module -- "$module_path"
cargo run --package tinymcp --example verify_migration -- "$module_path"
