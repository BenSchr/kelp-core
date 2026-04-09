"""Parallel metadata file loading helpers for the generic meta module."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from os.path import commonpath
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from kelp.meta.utils import _deep_merge_dicts


def collect_yaml_file_paths(
    paths: str | Path | Iterable[str | Path],
    *,
    patterns: tuple[str, ...] = ("*.yml", "*.yaml"),
    recursive: bool = True,
) -> list[Path]:
    """Collect YAML files from one or multiple paths.

    Args:
        paths: File or directory path(s).
        patterns: Glob patterns considered as YAML files.
        recursive: Whether to search directories recursively.

    Returns:
        Stable, deduplicated list of YAML file paths.

    """
    if isinstance(paths, (str, Path)):
        path_list: list[Path] = [Path(paths)]
    else:
        path_list = [Path(path) for path in paths]

    discovered: list[Path] = []
    for path in path_list:
        if path.is_file():
            discovered.append(path)
            continue

        if not path.is_dir():
            raise ValueError(f"Path {path} is neither a file nor a directory.")

        for pattern in patterns:
            if recursive:
                discovered.extend(path.rglob(pattern))
            else:
                discovered.extend(path.glob(pattern))

    deduped: dict[Path, None] = {}
    for path in sorted(discovered):
        deduped[path] = None

    return list(deduped.keys())


def _render_and_parse_yaml_file(
    file_path: Path,
    *,
    base_dir: Path,
    env: Environment,
    jinja_context: dict[str, Any],
    origin_file_path_key: str,
) -> dict[str, Any]:
    relative_path = file_path.relative_to(base_dir)
    template = env.get_template(str(relative_path))
    rendered = template.render(**jinja_context)
    parsed = yaml.safe_load(rendered) or {}
    return attach_origin_file_paths(
        parsed,
        relative_path=str(relative_path),
        origin_file_path_key=origin_file_path_key,
    )


def load_yaml_files_with_jinja_parallel(
    file_paths: Iterable[str | Path],
    *,
    jinja_context: dict[str, Any] | None = None,
    origin_file_path_key: str = "origin_file_path",
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Load and deep-merge YAML files with Jinja rendering in parallel.

    Args:
        file_paths: YAML file list to load.
        jinja_context: Variables available to Jinja rendering.
        origin_file_path_key: Metadata key storing source file path on objects.
        base_dir: Optional explicit base directory for computing origin_file_path.
                 If not provided, automatically computed from file paths (may truncate
                 paths for single deeply nested files). Pass explicitly to preserve
                 full folder structure in origin_file_path.

    Returns:
        Deep-merged YAML payload.

    """
    files = [Path(path) for path in file_paths]
    if not files:
        return {}

    if base_dir is None:
        base_dir = Path(commonpath([str(path) for path in files]))
        if base_dir.is_file():
            base_dir = base_dir.parent
    else:
        base_dir = Path(base_dir)

    env = Environment(
        loader=FileSystemLoader(str(base_dir)),
        undefined=StrictUndefined,
        variable_start_string="${",
        variable_end_string="}",
        autoescape=True,
    )

    render_func = partial(
        _render_and_parse_yaml_file,
        base_dir=base_dir,
        env=env,
        jinja_context=jinja_context or {},
        origin_file_path_key=origin_file_path_key,
    )

    merged: dict[str, Any] = {}
    with ThreadPoolExecutor() as executor:
        for parsed in executor.map(render_func, files):
            merged = _deep_merge_dicts(merged, parsed)

    return merged


def attach_origin_file_paths(
    parsed_yaml: dict[str, Any],
    *,
    relative_path: str,
    origin_file_path_key: str = "origin_file_path",
) -> dict[str, Any]:
    """Attach origin path metadata to list items under all root keys.

    Args:
        parsed_yaml: Parsed YAML payload.
        relative_path: Relative source path used for metadata attribution.
        origin_file_path_key: Metadata field to write onto each list item.

    Returns:
        Updated YAML payload.

    """
    for value in parsed_yaml.values():
        if isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    entry[origin_file_path_key] = relative_path
    return parsed_yaml
