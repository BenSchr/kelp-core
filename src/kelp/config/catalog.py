"""Catalog configuration parsing and model extraction.

This module handles parsing of catalog models from raw configuration data,
applying project-level and folder-level defaults, and creating Table and MetricView models.
"""

import logging
from typing import Any

from kelp.config.catalog_spec import CatalogParseSpec
from kelp.models.catalog import Catalog
from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive

logger = logging.getLogger(f"{__name__}")


def parse_catalog(
    raw_objects: dict[str, list],
    project_object_configs: dict[str, dict],
    specs: tuple[CatalogParseSpec, ...],
    project_root: str | None = None,
) -> Catalog:
    parsed_catalog_payload: dict[str, list] = {
        "models": [],
        "metric_views": [],
        "functions": [],
        "abacs": [],
    }

    for spec in specs:
        raw_items = raw_objects.get(spec.root_key, [])
        project_cfg = project_object_configs.get(spec.project_config_key, {})

        parsed_items: list = []
        for raw_item in raw_items:
            parsed_item_dict: dict[str, Any] = {}
            origin_file_path = raw_item.get("origin_file_path")
            parsed_item_dict["origin_file_path"] = origin_file_path
            logger.debug("Processing %s from file: %s", spec.model_label, origin_file_path)

            merged_item = apply_cfg_hierarchy_to_dict_recursive(
                raw_item,
                project_cfg,
                tpl_path=origin_file_path,
            )
            parsed_item_dict.update(merged_item)
            parsed_item_dict = spec.preprocess(parsed_item_dict, project_root)

            if parsed_item_dict.get("name") in (None, ""):
                parsed_item_dict["name"] = merged_item.get("name", "<unknown>")

            try:
                parsed_items.append(spec.model_class(**parsed_item_dict))
            except Exception as e:
                logger.error(
                    "Failed to parse %s '%s' in catalog data: %s",
                    spec.model_label,
                    parsed_item_dict.get("name", "<unknown>"),
                    e,
                )
                raise e

        parsed_catalog_payload[spec.catalog_attr] = parsed_items

    return Catalog(**parsed_catalog_payload)
