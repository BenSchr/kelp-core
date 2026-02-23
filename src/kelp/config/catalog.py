"""Catalog configuration parsing and model extraction.

This module handles parsing of catalog models from raw configuration data,
applying project-level and folder-level defaults, and creating Table models.
"""

import logging

from kelp.models.catalog import Catalog
from kelp.models.table import Table
from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive

logger = logging.getLogger(f"{__name__}")


def parse_catalog(raw_models: list, project_models_config: dict) -> Catalog:
    final_tables: list[Table] = []
    # models = raw_catalog.get("kelp_models", [])
    for model_data in raw_models:
        table_dict = {}
        origin_file_path = model_data.get("origin_file_path")
        table_dict["origin_file_path"] = origin_file_path
        logger.debug("Processing model from file: %s", origin_file_path)
        # apply project-level and folder-level defaults using recursive helper
        model_data = apply_cfg_hierarchy_to_dict_recursive(
            model_data, project_models_config, tpl_path=origin_file_path
        )
        table_dict.update(model_data)
        if "name" not in table_dict or table_dict.get("name") in (None, ""):
            table_dict["name"] = model_data.get("name", "<unknown>")
        try:
            tbl = Table(**table_dict)
            final_tables.append(tbl)
        except Exception as e:
            logger.error(
                "Failed to parse Table '%s' in catalog data: %s",
                table_dict.get("name", "<unknown>"),
                e,
            )
            raise e

    return Catalog(models=final_tables)
