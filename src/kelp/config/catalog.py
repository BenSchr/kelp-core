"""Catalog configuration parsing and model extraction.

This module handles parsing of catalog models from raw configuration data,
applying project-level and folder-level defaults, and creating Table and MetricView models.
"""

import logging

from kelp.models.catalog import Catalog
from kelp.models.metric_view import MetricView
from kelp.models.table import Table
from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive

logger = logging.getLogger(f"{__name__}")


def parse_catalog(
    raw_models: list,
    project_models_config: dict,
    raw_metrics: list | None = None,
    project_metrics_config: dict | None = None,
) -> Catalog:
    final_tables: list[Table] = []
    # models = raw_catalog.get("kelp_models", [])
    for model_data in raw_models:
        table_dict = {}
        origin_file_path = model_data.get("origin_file_path")
        table_dict["origin_file_path"] = origin_file_path
        logger.debug("Processing model from file: %s", origin_file_path)
        # apply project-level and folder-level defaults using recursive helper
        model_data = apply_cfg_hierarchy_to_dict_recursive(
            model_data,
            project_models_config,
            tpl_path=origin_file_path,
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

    # Parse metric views
    final_metrics: list[MetricView] = []
    if raw_metrics:
        project_metrics_config = project_metrics_config or {}
        for metric_data in raw_metrics:
            metric_dict = {}
            origin_file_path = metric_data.get("origin_file_path")
            metric_dict["origin_file_path"] = origin_file_path
            logger.debug("Processing metric view from file: %s", origin_file_path)
            # apply project-level and folder-level defaults using recursive helper
            metric_data = apply_cfg_hierarchy_to_dict_recursive(
                metric_data,
                project_metrics_config,
                tpl_path=origin_file_path,
            )
            metric_dict.update(metric_data)
            if "name" not in metric_dict or metric_dict.get("name") in (None, ""):
                metric_dict["name"] = metric_data.get("name", "<unknown>")
            try:
                metric = MetricView(**metric_dict)
                final_metrics.append(metric)
            except Exception as e:
                logger.error(
                    "Failed to parse MetricView '%s' in catalog data: %s",
                    metric_dict.get("name", "<unknown>"),
                    e,
                )
                raise e

    return Catalog(models=final_tables, metric_views=final_metrics)
