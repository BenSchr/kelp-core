"""Generic catalog assembly for meta frameworks."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kelp.meta.hierarchy import apply_hierarchy_defaults
from kelp.meta.spec import MetaObjectSpec

logger = logging.getLogger(__name__)


def _strip_path_prefix(file_path: str | Path | None, prefix: str) -> str | Path | None:
    """Strip leading directory component if it matches the prefix.

    This ensures that hierarchy relative paths are relative to the metadata
    directory, not including the metadata directory name itself.

    Example:
        _strip_path_prefix('models/bronze/customers.yml', 'models')
        => 'bronze/customers.yml'
    """
    if file_path is None:
        return None
    path = Path(file_path)
    parts = path.parts
    if parts and parts[0] == prefix:
        return str(Path(*parts[1:]))
    return file_path


def _settings_to_dict(project_settings: BaseModel | dict[str, Any]) -> dict[str, Any]:
    if isinstance(project_settings, BaseModel):
        return project_settings.model_dump()
    return project_settings


def build_catalog(
    *,
    raw_objects: dict[str, list[dict[str, Any]]],
    project_settings: BaseModel | dict[str, Any],
    object_specs: tuple[MetaObjectSpec, ...],
    project_root: str | None = None,
    origin_file_path_key: str = "origin_file_path",
) -> dict[str, list[Any]]:
    """Build validated catalog payload from raw objects and framework specs.

    Args:
        raw_objects: Raw object payload keyed by root key.
        project_settings: Framework settings model or dict.
        object_specs: Object type specifications.
        project_root: Optional project root for preprocess functions.
        origin_file_path_key: Item key containing relative source file path.

    Returns:
        Catalog payload keyed by each spec's ``catalog_attr``.

    Raises:
        Exception: Re-raises validation/preprocessing errors with context.

    """
    settings_dict = _settings_to_dict(project_settings)
    catalog_payload: dict[str, list[Any]] = {spec.catalog_attr: [] for spec in object_specs}

    for spec in object_specs:
        raw_items = raw_objects.get(spec.root_key, [])
        hierarchy_cfg = settings_dict.get(spec.project_config_key, {})
        parsed_items: list[Any] = []

        for raw_item in raw_items:
            origin_file_path = raw_item.get(origin_file_path_key)

            # Strip the project config key prefix from origin_file_path for hierarchy traversal
            # This ensures paths like 'models/bronze/file.yml' become 'bronze/file.yml'
            # when applying hierarchy defaults from the 'models' config section
            hierarchy_origin_path = _strip_path_prefix(
                origin_file_path,
                spec.project_config_key,
            )

            merged_item = apply_hierarchy_defaults(
                dict(raw_item),
                hierarchy_cfg,
                origin_file_path=hierarchy_origin_path,
            )
            preprocessed_item = spec.preprocess(merged_item, project_root)

            try:
                parsed_items.append(spec.model_class(**preprocessed_item))
            except Exception as error:
                logger.error(
                    "Failed to parse %s '%s': %s",
                    spec.model_label,
                    preprocessed_item.get("name", "<unknown>"),
                    error,
                )
                raise

        catalog_payload[spec.catalog_attr] = parsed_items

    return catalog_payload
