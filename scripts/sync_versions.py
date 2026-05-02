"""Sync the kelp-core version from pyproject.toml to all references in the repo.

Usage:
    uv run python scripts/sync_versions.py

This script reads the canonical version from pyproject.toml and updates:
  - Markdown files: kelp-core==<version> references
  - Makefile: kelp_core-<version>-py3-none-any.whl reference
  - Databricks bundle YAMLs: kelp_core-<version>-py3-none-any.whl references
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent

# Each entry: (glob pattern relative to REPO_ROOT, regex, replacement template)
# The replacement template uses {version} as a placeholder.
VERSION_PATTERNS: list[tuple[str, str, str]] = [
    ("**/*.md", r"kelp-core==\d+\.\d+\.\d+", "kelp-core=={version}"),
    (
        "Makefile",
        r"kelp_core-\d+\.\d+\.\d+-py3-none-any\.whl",
        "kelp_core-{version}-py3-none-any.whl",
    ),
    (
        "demos/**/*.yml",
        r"kelp_core-\d+\.\d+\.\d+-py3-none-any\.whl",
        "kelp_core-{version}-py3-none-any.whl",
    ),
]


def get_project_version() -> str:
    """Read the version from pyproject.toml."""
    pyproject = REPO_ROOT / "pyproject.toml"
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    return data["project"]["version"]


def sync_all(version: str) -> list[Path]:
    """Apply all version patterns across the repo.

    Args:
        version: Target version string (e.g. "0.0.7").

    Returns:
        List of files that were modified.
    """
    modified: list[Path] = []

    for glob, regex, template in VERSION_PATTERNS:
        pattern = re.compile(regex)
        replacement = template.format(version=version)

        for path in sorted(REPO_ROOT.glob(glob)):
            if not path.is_file():
                continue
            original = path.read_text(encoding="utf-8")
            updated = pattern.sub(replacement, original)
            if updated != original:
                path.write_text(updated, encoding="utf-8")
                modified.append(path)

    return modified


def main() -> None:
    version = get_project_version()
    print(f"Syncing version: {version}")

    modified = sync_all(version)
    for path in modified:
        print(f"  Updated: {path.relative_to(REPO_ROOT)}")

    if not modified:
        print("  All files already up to date.")

    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
