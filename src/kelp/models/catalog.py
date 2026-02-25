# from pathlib import Path

import logging

from pydantic import BaseModel, Field, PrivateAttr

# from kelp.models.project_config import ProjectConfig
from kelp.models.metric_view import MetricView
from kelp.models.table import Table

# from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive
# from kelp.utils.jinja_parser import load_yaml_with_jinja


logger = logging.getLogger(f"{__name__}")


class Catalog(BaseModel):
    """Catalog of tables and metric views defined in the kelp project."""

    models: list[Table] = Field(default_factory=list)
    metric_views: list[MetricView] = Field(default_factory=list)
    _table_index_cache: dict[str, Table] = PrivateAttr(default_factory=dict)
    _metrics_index_cache: dict[str, MetricView] = PrivateAttr(default_factory=dict)
    _table_index_built: bool = PrivateAttr(default=False)
    _metrics_index_built: bool = PrivateAttr(default=False)

    # --- Index helpers -------------------------------------------------
    def _build_table_index(self) -> None:
        """Build name -> Table index and record duplicates.

        Policy: keep-first for duplicate names and log a warning when a
        duplicate name is encountered.
        """
        index: dict[str, Table] = {}

        for tbl in self.models:
            name = getattr(tbl, "name", None) or "<unknown>"
            if name in index:
                # duplicate found — log and keep the first occurrence
                logger.warning("Duplicate table name encountered: %s (kept first occurrence)", name)
                continue
            index[name] = tbl

        # Cache on the instance using the private field
        self._table_index_cache = index
        self._table_index_built = True

    def _build_metrics_index(self) -> None:
        """Build name -> MetricView index and record duplicates.

        Policy: keep-first for duplicate names and log a warning when a
        duplicate name is encountered.
        """
        index: dict[str, MetricView] = {}

        for metric in self.metric_views:
            name = getattr(metric, "name", None) or "<unknown>"
            if name in index:
                # duplicate found — log and keep the first occurrence
                logger.warning(
                    "Duplicate metric view name encountered: %s (kept first occurrence)",
                    name,
                )
                continue
            index[name] = metric

        # Cache on the instance using the private field
        self._metrics_index_cache = index
        self._metrics_index_built = True

    @property
    def table_index(self) -> dict[str, Table]:
        """Return a mapping name -> Table (keeps first occurrence on dupes)."""
        if not self._table_index_built:
            self._build_table_index()
        return self._table_index_cache

    @property
    def metrics_index(self) -> dict[str, MetricView]:
        """Return a mapping name -> MetricView (keeps first occurrence on dupes)."""
        if not self._metrics_index_built:
            self._build_metrics_index()
        return self._metrics_index_cache

    def get_table(self, name: str, soft_handle: bool = False) -> Table:
        """Return the first Table matching `name` or None if not found."""
        table = self.table_index.get(name)
        if not table and not soft_handle:
            raise KeyError(f"Table not found in catalog: {name}")
        if not table and soft_handle:
            logger.warning(
                "Table not found in catalog: %s. Returning placeholder table since soft_handle=True.",
                name,
            )
            table = Table(name=name)
        if table is None:
            raise KeyError(f"Table not found in catalog: {name}")
        return table

    def get_metric_view(self, name: str, soft_handle: bool = False) -> MetricView:
        """Return the first MetricView matching `name` or None if not found."""
        metric = self.metrics_index.get(name)
        if not metric and not soft_handle:
            raise KeyError(f"Metric view not found in catalog: {name}")
        if not metric and soft_handle:
            logger.warning(
                "Metric view not found in catalog: %s. Returning placeholder metric since soft_handle=True.",
                name,
            )
            metric = MetricView(name=name)
        if metric is None:
            raise KeyError(f"Metric view not found in catalog: {name}")
        return metric

    def get_tables(self) -> list[Table]:
        """Return all Tables in the catalog as a list."""
        return self.models

    def get_metric_views(self) -> list[MetricView]:
        """Return all MetricViews in the catalog as a list."""
        return self.metric_views

    def refresh_index(self) -> None:
        """Rebuild the internal indices from `self.models` and `self.metric_views`.

        Call this if `self.models` or `self.metric_views` has been modified after construction.
        """
        self._table_index_cache = {}
        self._metrics_index_cache = {}
        self._table_index_built = False
        self._metrics_index_built = False
        self._build_table_index()
        self._build_metrics_index()
