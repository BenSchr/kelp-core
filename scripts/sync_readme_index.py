"""Sync README.md and docs/index.md to the same content.

Usage:
    uv run python scripts/sync_readme_index.py

Behavior:
- Treats README.md as the source of truth.
- Ensures a warning note appears above the guides links table.
- Writes the normalized content to both README.md and docs/index.md.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
README = REPO_ROOT / "README.md"
DOCS_INDEX = REPO_ROOT / "docs" / "index.md"

LINKS_TABLE_HEADER = "| Guide | Overview |"
LINKS_WARNING = (
    "> ⚠ Some links in the table below may not work in repository preview contexts.\n"
    "> Please use the docs website for reliable navigation: https://benschr.github.io/kelp-core/"
)


def _ensure_warning_above_links_table(content: str) -> str:
    """Ensure the links warning exists directly above the guides table.

    Args:
        content: Markdown content.

    Returns:
        Updated content with warning inserted when needed.
    """
    if LINKS_TABLE_HEADER not in content:
        return content

    if LINKS_WARNING in content:
        return content

    return content.replace(
        LINKS_TABLE_HEADER,
        f"{LINKS_WARNING}\n\n{LINKS_TABLE_HEADER}",
        1,
    )


def main() -> None:
    """Synchronize README and docs index content."""
    readme_content = README.read_text(encoding="utf-8")
    normalized = _ensure_warning_above_links_table(readme_content)

    README.write_text(normalized, encoding="utf-8")
    DOCS_INDEX.write_text(normalized, encoding="utf-8")

    print("Updated: README.md")
    print("Updated: docs/index.md")


if __name__ == "__main__":
    main()
