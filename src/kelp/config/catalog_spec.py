"""Shared catalog parse specifications for runtime/catalog."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kelp.models.abac import AbacPolicy
from kelp.models.function import KelpFunction
from kelp.models.metric_view import MetricView
from kelp.models.source import Source
from kelp.models.table import Table


@dataclass(frozen=True)
class CatalogParseSpec:
    """Specification for loading and parsing a kelp object type."""

    root_key: str
    project_config_key: str
    path_attr: str
    model_class: type
    catalog_attr: str
    model_label: str
    preprocess: Callable[[dict[str, Any], str | None], dict[str, Any]]


def _noop_preprocess(item_dict: dict[str, Any], project_root: str | None) -> dict[str, Any]:
    return item_dict


def _function_preprocess(item_dict: dict[str, Any], project_root: str | None) -> dict[str, Any]:
    if item_dict.get("body_path") and not item_dict.get("body"):
        if project_root is None:
            raise ValueError(
                "project_root is required when using 'body_path' in kelp_functions",
            )
        body_file = Path(project_root).joinpath(item_dict["body_path"])
        if not body_file.exists():
            raise FileNotFoundError(f"Function body file not found: {body_file}")
        item_dict["body"] = body_file.read_text(encoding="utf-8")
    return item_dict


CATALOG_PARSE_SPECS: tuple[CatalogParseSpec, ...] = (
    CatalogParseSpec(
        root_key="kelp_models",
        project_config_key="models",
        path_attr="models_path",
        model_class=Table,
        catalog_attr="models",
        model_label="Table",
        preprocess=_noop_preprocess,
    ),
    CatalogParseSpec(
        root_key="kelp_metric_views",
        project_config_key="metric_views",
        path_attr="metrics_path",
        model_class=MetricView,
        catalog_attr="metric_views",
        model_label="MetricView",
        preprocess=_noop_preprocess,
    ),
    CatalogParseSpec(
        root_key="kelp_functions",
        project_config_key="functions",
        path_attr="functions_path",
        model_class=KelpFunction,
        catalog_attr="functions",
        model_label="KelpFunction",
        preprocess=_function_preprocess,
    ),
    CatalogParseSpec(
        root_key="kelp_abacs",
        project_config_key="abacs",
        path_attr="abacs_path",
        model_class=AbacPolicy,
        catalog_attr="abacs",
        model_label="AbacPolicy",
        preprocess=_noop_preprocess,
    ),
    CatalogParseSpec(
        root_key="kelp_sources",
        project_config_key="sources",
        path_attr="sources_path",
        model_class=Source,
        catalog_attr="sources",
        model_label="Source",
        preprocess=_noop_preprocess,
    ),
)
