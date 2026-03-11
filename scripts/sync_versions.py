"""Sync the kelp-core version from pyproject.toml to all docs and source references.

Usage:
    uv run python scripts/sync_versions.py

This script reads the canonical version from pyproject.toml and updates:
  - README.md
  - docs/index.md
  - Any *.md file under docs/ containing a kelp-core==<version> reference
  - src/kelp/cli/version.py (the hardcoded version string)
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


def get_project_version() -> str:
    """Read the version from pyproject.toml.

    Returns:
        The current project version string.
    """
    pyproject = REPO_ROOT / "pyproject.toml"
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    return data["project"]["version"]


def replace_in_file(path: Path, pattern: re.Pattern[str], replacement: str) -> bool:
    """Replace all matches of pattern with replacement in a file.

    Args:
        path: File to modify.
        pattern: Compiled regex pattern.
        replacement: Replacement string (may reference capture groups).

    Returns:
        True if any replacements were made, False otherwise.
    """
    original = path.read_text(encoding="utf-8")
    updated = pattern.sub(replacement, original)
    if updated != original:
        path.write_text(updated, encoding="utf-8")
        return True
    return False


def sync_markdown_version(version: str) -> list[Path]:
    """Update kelp-core==<version> references in all markdown files.

    Args:
        version: Target version string (e.g. "0.0.3").

    Returns:
        List of files that were modified.
    """
    pattern = re.compile(r"kelp-core==\d+\.\d+\.\d+")
    replacement = f"kelp-core=={version}"

    targets = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "docs" / "index.md",
        *(REPO_ROOT / "docs").rglob("*.md"),
    ]
    # Deduplicate while preserving order
    seen: set[Path] = set()
    unique_targets: list[Path] = []
    for p in targets:
        if p not in seen and p.exists():
            seen.add(p)
            unique_targets.append(p)

    modified: list[Path] = [
        path for path in unique_targets if replace_in_file(path, pattern, replacement)
    ]

    return modified


def sync_version_py(version: str) -> bool:
    """Update the hardcoded version string in src/kelp/cli/version.py.

    Args:
        version: Target version string.

    Returns:
        True if the file was modified.
    """
    version_py = REPO_ROOT / "src" / "kelp" / "cli" / "version.py"
    # Read the version at runtime from importlib.metadata instead of hardcoding
    original = version_py.read_text(encoding="utf-8")

    # Replace the hardcoded version string (e.g. "Kelp version: 0.0.0")
    pattern = re.compile(r'("Kelp version: )\d+\.\d+\.\d+(")')
    updated = pattern.sub(rf"\g<1>{version}\g<2>", original)

    if updated != original:
        version_py.write_text(updated, encoding="utf-8")
        return True
    return False


def main() -> None:
    version = get_project_version()
    print(f"Syncing version: {version}")

    md_modified = sync_markdown_version(version)
    for path in md_modified:
        print(f"  Updated: {path.relative_to(REPO_ROOT)}")

    if sync_version_py(version):
        print("  Updated: src/kelp/cli/version.py")

    if not md_modified:
        print("  No markdown files required updates.")

    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
