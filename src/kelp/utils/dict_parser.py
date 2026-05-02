import copy
from pathlib import Path


def _merge_with_precedence(base: dict, override: dict) -> dict:
    """Deep-merge two dicts where ``override`` wins for conflicts.

    This is used to build an effective defaults object from hierarchy layers
    (top-level first, then deeper folders). Explicit model values are still
    protected later by ``merge_defaults``.
    """
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_with_precedence(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _extract_plus_defaults(cfg_node: dict) -> dict:
    """Extract keys prefixed with ``+`` and strip the prefix."""
    if not isinstance(cfg_node, dict):
        return {}

    defaults: dict = {}
    for key, value in cfg_node.items():
        if isinstance(key, str) and key.startswith("+"):
            defaults[key[1:]] = value
    return defaults


def merge_defaults(target: dict, defaults: dict) -> dict:
    """Merge defaults into target without overwriting existing values.

    - If a key is missing in target, a deep copy of the default value is set.
    - If both target and default values are dicts, merge recursively.
    - If both are lists, extend the target list with default items that are not
      already present (preserves existing items from target).
    - Otherwise, leave the existing target value untouched.
    """
    for k, v in defaults.items():
        if k not in target:
            # set a deepcopy to avoid accidental shared mutable structures
            target[k] = copy.deepcopy(v)
            continue

        tv = target[k]
        # both dicts -> recurse
        if isinstance(tv, dict) and isinstance(v, dict):
            merge_defaults(tv, v)
            continue

        # both lists -> merge items that aren't already present
        if isinstance(tv, list) and isinstance(v, list):
            # For lists of primitive values or dicts, avoid duplicates by equality
            for item in v:
                if item not in tv:
                    # Defaults are applied before existing items to keep explicit
                    # model values later in the list; insert at start.
                    tv.insert(0, copy.deepcopy(item))
            continue

        # otherwise: keep existing target value (do not overwrite)
    return target


def apply_cfg_hierarchy_to_dict_recursive(
    target: dict,
    cfg: dict,
    tpl_path: str | Path | None = None,
) -> dict:
    """Apply a configuration hierarchy onto `target` (recursive folder +defaults)."""
    if not isinstance(target, dict):
        return target

    if not isinstance(cfg, dict):
        return target

    # Build effective defaults by walking the hierarchy path:
    # top-level +keys, then each matching folder in order.
    effective_defaults = _extract_plus_defaults(cfg)

    if tpl_path is not None:
        parts = Path(str(tpl_path)).parent.parts
        current_cfg = cfg

        for part in parts:
            next_cfg = current_cfg.get(part)
            if not isinstance(next_cfg, dict):
                break

            folder_defaults = _extract_plus_defaults(next_cfg)
            effective_defaults = _merge_with_precedence(effective_defaults, folder_defaults)
            current_cfg = next_cfg

    # Apply computed defaults without overwriting explicit values in target.
    merge_defaults(target, effective_defaults)

    return target
