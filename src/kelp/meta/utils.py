"""Internal utility functions for the meta module (no external dependencies outside stdlib/pydantic/yaml/jinja2)."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

# ============================================================================
# Dict/Hierarchy utilities
# ============================================================================


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
            target[k] = copy.deepcopy(v)
            continue

        tv = target[k]
        # both dicts -> recurse
        if isinstance(tv, dict) and isinstance(v, dict):
            merge_defaults(tv, v)
            continue

        # both lists -> merge items that aren't already present
        if isinstance(tv, list) and isinstance(v, list):
            for item in v:
                if item not in tv:
                    tv.insert(0, copy.deepcopy(item))
            continue

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


# ============================================================================
# Jinja/Template utilities
# ============================================================================


def render_string_with_jinja(
    s: str,
    jinja_context: dict[str, Any] | None = None,
    jinja_env: Environment | None = None,
) -> str:
    """Render a single string as a Jinja template with the provided context."""
    if not isinstance(s, str):
        return s
    local_env = jinja_env or Environment(
        undefined=StrictUndefined,
        variable_start_string="${",
        variable_end_string="}",
        autoescape=True,
    )

    try:
        template = local_env.from_string(s)
        return template.render(**(jinja_context or {}))
    except Exception as e:
        raise ValueError(f"Failed to render string as Jinja template: {e}") from e


def render_obj_with_jinja(
    obj: Any,
    jinja_context: dict[str, Any] | None = None,
    jinja_env: Environment | None = None,
) -> Any:
    """Recursively render an object (string, dict, list) with Jinja templates."""
    if isinstance(obj, str):
        return render_string_with_jinja(obj, jinja_context, jinja_env)
    if isinstance(obj, dict):
        return {k: render_obj_with_jinja(v, jinja_context, jinja_env) for k, v in obj.items()}
    if isinstance(obj, list):
        return [render_obj_with_jinja(v, jinja_context, jinja_env) for v in obj]
    return obj


def _deep_merge_dicts(result: dict, parsed_yaml: dict) -> dict:
    """Deep merge two dictionaries, extending lists and recursing on nested dicts."""
    for key, value in parsed_yaml.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_dicts(result[key], value)
        elif key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key].extend(value)
        else:
            result[key] = value
    return result


def _render_jinja(
    rel_path: str,
    context: dict[str, Any],
    env: Environment,
) -> str:
    """Render a Jinja template by relative path."""
    try:
        template = env.get_template(rel_path)
    except TemplateNotFound as e:
        raise FileNotFoundError(f"Template not found: {rel_path}") from e
    return template.render(**context)


def load_yaml_with_jinja(
    path: str | Path,
    jinja_context: dict[str, Any] | None = None,
    jinja_env: Environment | None = None,
    patterns: tuple[str, ...] = ("*.yml", "*.yaml"),
    recursive: bool = True,
) -> dict[str, Any]:
    """Load YAML file(s) with Jinja rendering support."""
    path = Path(path)
    context = jinja_context or {}

    base_dir = path if path.is_dir() else path.parent
    loader = FileSystemLoader(str(base_dir))
    env = jinja_env or Environment(
        loader=loader,
        undefined=StrictUndefined,
        variable_start_string="${",
        variable_end_string="}",
        autoescape=True,
    )

    # Collect files to process
    files: list[Path]
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = []
        for pattern in patterns:
            if recursive:
                files.extend(base_dir.rglob(pattern))
            else:
                files.extend(base_dir.glob(pattern))
        seen = set()
        files = [f for f in files if not (f in seen or seen.add(f))]
    else:
        raise ValueError(f"Path {path} is neither a file nor a directory.")

    def load_and_render(file_path: Path) -> dict[str, Any]:
        relative_path = file_path.relative_to(base_dir)
        rendered_content = _render_jinja(str(relative_path), context, env)
        try:
            parsed_yaml = yaml.safe_load(rendered_content) or {}
        except Exception as e:
            raise ValueError(
                f"Failed to parse YAML from rendered template {relative_path}: {e}",
            ) from e
        return parsed_yaml

    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    result: dict[str, Any] = {}
    loader_func = partial(load_and_render)
    with ThreadPoolExecutor() as executor:
        for parsed in executor.map(loader_func, files):
            result = _deep_merge_dicts(result, parsed)

    return result


# ============================================================================
# YAML utilities
# ============================================================================


def load_yaml(file_path: str | Path) -> dict[str, Any]:
    """Load a YAML file from the given path."""
    if isinstance(file_path, str):
        file_path = Path(file_path)
    text = file_path.read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


# ============================================================================
# Path/Search utilities
# ============================================================================


def find_path_by_name(
    start_path: str | Path,
    target_name: str,
    search_strategy: str = "both",
    max_depth_up: int = 3,
    max_depth_down: int = 3,
) -> Path | None:
    """Find a file or directory by name using configurable search strategy.

    Args:
        start_path: Starting path for the search.
        target_name: Name of file or directory to find.
        search_strategy: "up", "down", or "both".
        max_depth_up: Max levels to traverse upward.
        max_depth_down: Max levels to traverse downward.

    Returns:
        Path object if found, None otherwise.
    """
    start_path = Path(start_path).resolve()

    # Search upward (parents)
    if search_strategy in ("up", "both"):
        current = start_path
        for _ in range(max_depth_up):
            candidate = current / target_name
            if candidate.exists():
                return candidate
            parent = current.parent
            if parent == current:
                break
            current = parent

    # Search downward (children)
    if search_strategy in ("down", "both"):

        def _search_recursive(path: Path, depth: int) -> Path | None:
            if depth <= 0:
                return None

            try:
                for item in path.iterdir():
                    if item.name == target_name:
                        return item
                    if item.is_dir():
                        result = _search_recursive(item, depth - 1)
                        if result:
                            return result
            except (PermissionError, OSError):
                pass

            return None

        result = _search_recursive(start_path, max_depth_down)
        if result:
            return result

    return None
