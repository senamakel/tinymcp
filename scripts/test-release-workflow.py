#!/usr/bin/env python3
"""Behaviorally check TinyMCP's main and reviewed-epic release dispatch paths."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/release.yml"
MAIN_RELEASE_PIN = "4adc1080fb495098e7af1aade82ed7e6e3c69bb7"
REVIEWED_RELEASE_PIN = "1e28317c435a9c714002199e65d18a1067fa5777"
MODULE_CI_PIN = "62c8ae0468f3882b75947d5cba9687e609ce1fed"


def resolver_script(workflow_text: str) -> str:
    marker = "      - id: tag\n"
    if marker not in workflow_text:
        raise AssertionError("release workflow has no tag resolver step")
    resolver = workflow_text.split(marker, 1)[1]
    run_marker = "        run: |\n"
    if run_marker not in resolver:
        raise AssertionError("release tag resolver has no shell block")
    block = resolver.split(run_marker, 1)[1]
    script_lines: list[str] = []
    for line in block.splitlines():
        if line and not line.startswith("          "):
            break
        script_lines.append(line[10:] if line.startswith("          ") else "")
    if not script_lines:
        raise AssertionError("release tag resolver shell block is empty")
    return "\n".join(script_lines) + "\n"


def resolve(tag: str, mode: str, reviewed_pr: str) -> tuple[int, dict[str, str], str]:
    script = resolver_script(WORKFLOW.read_text())
    with tempfile.TemporaryDirectory(prefix="tinymcp-release-contract-") as directory:
        output = Path(directory) / "github-output.txt"
        env = {
            "PATH": os.environ.get("PATH", ""),
            "GITHUB_OUTPUT": str(output),
            "TAG": tag,
            "RELEASE_MODE": mode,
            "REVIEWED_PR": reviewed_pr,
        }
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        output_values = {}
        if output.exists():
            output_values = dict(
                line.split("=", 1) for line in output.read_text().splitlines() if "=" in line
            )
        return result.returncode, output_values, result.stderr


class ReleaseWorkflowContractTests(unittest.TestCase):
    def test_legacy_main_path_still_resolves_existing_tags_and_zero_pr(self) -> None:
        code, values, _stderr = resolve("v0.19.3", "main", "")
        self.assertEqual(code, 0)
        self.assertEqual(
            values,
            {
                "tag": "v0.19.3",
                "version": "0.19.3",
                "release_mode": "main",
                "reviewed_pr": "0",
            },
        )

    def test_reviewed_epic_requires_and_preserves_a_positive_integer_pr(self) -> None:
        code, values, _stderr = resolve("v0.19.3", "reviewed-epic", "8056")
        self.assertEqual(code, 0)
        self.assertEqual(values["reviewed_pr"], "8056")
        self.assertEqual(values["release_mode"], "reviewed-epic")

    def test_main_mode_refuses_a_reviewed_pr_number(self) -> None:
        code, values, stderr = resolve("v0.19.3", "main", "8056")
        self.assertEqual(code, 2)
        self.assertEqual(values, {})
        self.assertIn("only valid in reviewed-epic mode", stderr)

    def test_reviewed_mode_rejects_missing_zero_and_non_integer_prs(self) -> None:
        for reviewed_pr in ("", "0", "-1", "1.5", "abc"):
            with self.subTest(reviewed_pr=reviewed_pr):
                code, values, stderr = resolve("v0.19.1", "reviewed-epic", reviewed_pr)
                self.assertEqual(code, 2)
                self.assertEqual(values, {})
                self.assertIn("positive integer", stderr)

    def test_invalid_tag_or_mode_fails_before_any_release_output(self) -> None:
        for tag, mode, reviewed_pr in (
            ("0.19.3", "main", ""),
            ("v0.19.1", "other", ""),
        ):
            with self.subTest(tag=tag, mode=mode):
                code, values, _stderr = resolve(tag, mode, reviewed_pr)
                self.assertEqual(code, 2)
                self.assertEqual(values, {})

    def test_caller_keeps_main_pin_and_routes_reviewed_releases_through_owner(self) -> None:
        workflow = WORKFLOW.read_text()
        self.assertIn(
            "TAG: ${{ github.event_name == 'push' && github.ref_name || inputs.tag }}",
            workflow,
        )
        self.assertIn(
            "RELEASE_MODE: ${{ github.event_name == 'push' && 'main' || inputs.release_mode }}",
            workflow,
        )
        self.assertIn("REVIEWED_PR: ${{ inputs.reviewed_pr }}", workflow)
        self.assertIn(
            f"uses: tinyhumansai/.github/.github/workflows/module-release.yml@{MAIN_RELEASE_PIN}",
            workflow,
        )
        self.assertIn(
            "module-crate: tinymcp\n      asset-name: tinymcp\n"
            "      module-manifest: Cargo.toml\n      extra-packaged-files: '[]'",
            workflow,
        )
        self.assertIn(
            "uses: tinyhumansai/.github/.github/workflows/"
            f"module-reviewed-epic-release.yml@{REVIEWED_RELEASE_PIN}",
            workflow,
        )
        self.assertIn("reviewed-pr: ${{ fromJSON(needs.resolve.outputs.reviewed_pr) }}", workflow)
        self.assertIn("permissions: {}", workflow)
        self.assertIn("pull-requests: read", workflow)
        self.assertIn("checks: read", workflow)
        self.assertIn("actions: read", workflow)
        ci_workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn(
            f"uses: tinyhumansai/.github/.github/workflows/module-ci.yml@{MODULE_CI_PIN}",
            ci_workflow,
        )


if __name__ == "__main__":
    unittest.main()
