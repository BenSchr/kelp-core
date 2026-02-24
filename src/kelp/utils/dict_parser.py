import copy
from pathlib import Path


def merge_defaults(target, defaults):
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

    def _collect_plus_defaults(cfg_node: dict):
        global_defaults = {}
        folder_defaults = {}
        if not isinstance(cfg_node, dict):
            return global_defaults, folder_defaults

        # top-level global +keys
        for k, v in cfg_node.items():
            if isinstance(k, str) and k.startswith("+"):
                global_defaults[k[1:]] = v

        # recursively collect +keys from nested folder dicts
        def _recurse(node, name=None):
            if not isinstance(node, dict):
                return
            fd = {}
            for kk, vv in node.items():
                if isinstance(kk, str) and kk.startswith("+"):
                    fd[kk[1:]] = vv
            if fd and name:
                # last-wins for identical folder names encountered deeper
                folder_defaults[name] = fd
            for kk, vv in node.items():
                if isinstance(kk, str) and isinstance(vv, dict):
                    _recurse(vv, kk)

        for k, v in cfg_node.items():
            if isinstance(k, str) and isinstance(v, dict):
                _recurse(v, k)

        return global_defaults, folder_defaults

    global_defaults, folder_defaults = _collect_plus_defaults(cfg or {})

    # apply global defaults first
    merge_defaults(target, global_defaults)

    # apply folder defaults based on tpl_path parts
    if tpl_path is not None:
        parts = Path(str(tpl_path)).parent.parts
        for part in parts:
            if part in folder_defaults:
                merge_defaults(target, folder_defaults[part])

    return target
