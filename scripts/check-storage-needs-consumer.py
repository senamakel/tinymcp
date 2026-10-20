#!/usr/bin/env python3
"""Guard the standalone storage-needs consumer's normal/build dependency graph."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONSUMER_MANIFEST = ROOT / "crates/tinymcp-bus/consumer/Cargo.toml"
CONTRACT_MANIFEST = ROOT / "crates/tinymcp-bus/Cargo.toml"
TSD_BUS_REV = "d8ebef1a98df3918ed3b80719fdc1414ee20f7a4"
TSD_BUS_URL = "https://github.com/tinyhumansai/tinystoragedrivers"
ALLOWED_TSD_BUS = "tinystoragedrivers-bus"
ALLOWED_CONTRACT = "tinymcp-bus"

FORBIDDEN_PACKAGES = {
    "async-std",
    "async-executor",
    "async-io",
    "axum",
    "actix-web",
    "awc",
    "attohttpc",
    "diesel",
    "futures-executor",
    "hyper",
    "isahc",
    "libsqlite3-sys",
    "libsqlx-core",
    "minreq",
    "mongodb",
    "mysql",
    "postgres",
    "redb",
    "reqwest",
    "rocksdb",
    "rusqlite",
    "serde_rusqlite",
    "sled",
    "smol",
    "sqlx",
    "tinybus",
    "tinybus-macros",
    "tinybus-module",
    "tokio",
    "tokio-stream",
    "tinymcp",
    "tinystoragedrivers",
    "tinystoragedrivers-busproxy",
    "tinystoragedrivers-client",
    "tinystoragedrivers-core",
    "tinystoragedrivers-secrets",
    "tide",
    "ureq",
    "surf",
}


def fail(message: str) -> None:
    raise RuntimeError(message)


def _normal_build_dep_ids(node: dict[str, Any]) -> list[str]:
    dependencies = node.get("deps")
    if not isinstance(dependencies, list):
        fail(f"Cargo metadata node {node.get('id')!r} has no dependency list")

    result: list[str] = []
    for dependency in dependencies:
        package_id = dependency.get("pkg")
        kinds = dependency.get("dep_kinds")
        if not isinstance(package_id, str) or not isinstance(kinds, list) or not kinds:
            fail(f"Cargo metadata has an incomplete dependency edge in {node.get('id')!r}")
        for kind in kinds:
            if not isinstance(kind, dict) or "kind" not in kind:
                fail(f"Cargo metadata has an unknown dependency kind in {node.get('id')!r}")
            if kind["kind"] in (None, "build"):
                result.append(package_id)
                break
    return result


def validate_metadata(metadata: dict[str, Any]) -> None:
    """Fail unless the consumer resolves only the canonical pure contract graph."""
    packages = metadata.get("packages")
    resolve = metadata.get("resolve")
    if not isinstance(packages, list) or not isinstance(resolve, dict):
        fail("Cargo metadata is missing package or resolved-graph data")
    root_id = resolve.get("root")
    nodes = resolve.get("nodes")
    if not isinstance(root_id, str) or not isinstance(nodes, list):
        fail("Cargo metadata has no resolved consumer root")

    package_by_id = {
        package.get("id"): package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    node_by_id = {
        node.get("id"): node
        for node in nodes
        if isinstance(node, dict) and isinstance(node.get("id"), str)
    }
    if root_id not in package_by_id or root_id not in node_by_id:
        fail("Cargo metadata root package is absent from its resolved graph")

    reachable: set[str] = set()
    pending = [root_id]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        node = node_by_id.get(package_id)
        if node is None:
            fail(f"Cargo metadata has no resolved node for {package_id!r}")
        pending.extend(_normal_build_dep_ids(node))

    packages_reached = [package_by_id[package_id] for package_id in reachable]
    names = {package.get("name") for package in packages_reached}
    if ALLOWED_CONTRACT not in names or ALLOWED_TSD_BUS not in names:
        fail("consumer graph is missing tinymcp-bus or canonical TSD bus")
    if package_by_id[root_id].get("manifest_path") != str(CONSUMER_MANIFEST):
        fail("dependency guard did not inspect the excluded standalone consumer")

    for package_id in reachable - {root_id}:
        package = package_by_id[package_id]
        name = package.get("name")
        if not isinstance(name, str):
            fail(f"Cargo metadata package {package_id!r} has no name")
        if name in FORBIDDEN_PACKAGES or name.startswith("tinybus-"):
            fail(f"consumer normal/build graph contains forbidden package {name!r}")
        if name.startswith("tinymcp-") and name != ALLOWED_CONTRACT:
            fail(f"consumer normal/build graph contains implementation package {name!r}")
        if name.startswith("tinystoragedrivers-") and name != ALLOWED_TSD_BUS:
            fail(f"consumer normal/build graph contains storage implementation {name!r}")

    tsd_packages = [
        package
        for package in packages_reached
        if package.get("name") == ALLOWED_TSD_BUS
    ]
    if len(tsd_packages) != 1:
        fail("consumer and contract did not unify to one TSD bus package identity")
    tsd_package = tsd_packages[0]
    source = tsd_package.get("source")
    if (
        not isinstance(source, str)
        or not source.startswith(f"git+{TSD_BUS_URL}?")
        or f"rev={TSD_BUS_REV}" not in source
        or not source.endswith(f"#{TSD_BUS_REV}")
        or not str(tsd_package.get("version", "")).startswith("1.9.")
    ):
        fail("TSD bus did not resolve from the pinned v1.9.0 source revision")

    tsd_node = node_by_id[tsd_package["id"]]
    features = tsd_node.get("features")
    if not isinstance(features, list):
        fail("Cargo metadata omitted active TSD bus features")
    if "client" in features:
        fail("TSD bus client feature is enabled in the consumer graph")

    root_deps = _normal_build_dep_ids(node_by_id[root_id])
    contract_packages = [
        package
        for package in packages_reached
        if package.get("name") == ALLOWED_CONTRACT
    ]
    if len(contract_packages) != 1:
        fail("consumer did not resolve exactly one tinymcp-bus contract package")
    if (
        contract_packages[0].get("source") is not None
        or contract_packages[0].get("manifest_path") != str(CONTRACT_MANIFEST)
    ):
        fail("consumer is not using this checkout's tinymcp-bus contract")
    contract_node = node_by_id[contract_packages[0]["id"]]
    contract_deps = _normal_build_dep_ids(contract_node)
    if tsd_package["id"] not in root_deps or tsd_package["id"] not in contract_deps:
        fail("consumer and tinymcp-bus must share the same canonical TSD bus type")


def main() -> int:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--manifest-path",
        str(CONSUMER_MANIFEST),
    ]
    completed = subprocess.run(command, cwd=ROOT, check=False, text=True, capture_output=True)
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        return completed.returncode
    try:
        metadata = json.loads(completed.stdout)
        validate_metadata(metadata)
    except (json.JSONDecodeError, RuntimeError) as error:
        print(f"storage-needs consumer dependency guard failed: {error}", file=sys.stderr)
        return 1
    print("storage-needs consumer normal/build graph is pure and uses the canonical TSD type")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
