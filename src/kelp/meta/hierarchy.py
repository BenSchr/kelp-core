"""Hierarchy default application wrappers for generic metadata loading."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from kelp.meta.utils import (
    apply_cfg_hierarchy_to_dict_recursive,
    merge_defaults,
)


def apply_hierarchy_defaults(
    item: dict[str, Any],
    hierarchy_config: dict[str, Any],
    *,
    origin_file_path: str | Path | None = None,
) -> dict[str, Any]:
    """Apply folder hierarchy defaults to one metadata item.

    Defaults use ``+`` prefix keys and are merged without overriding explicit
    values on the item payload.

    Args:
        item: Metadata item dictionary.
        hierarchy_config: Framework hierarchy defaults mapping.
        origin_file_path: Relative path of the source file for folder traversal.

    Returns:
        Item dictionary with hierarchy defaults applied.

    """
    return apply_cfg_hierarchy_to_dict_recursive(
        item,
        hierarchy_config,
        tpl_path=origin_file_path,
    )


def merge_item_defaults(item: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Merge default values into an item payload.

    Args:
        item: Target dictionary.
        defaults: Default values dictionary.

    Returns:
        Updated target dictionary.

    """
    return merge_defaults(item, defaults)
