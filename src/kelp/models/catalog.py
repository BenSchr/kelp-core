# from pathlib import Path

import logging

from pydantic import BaseModel, Field, PrivateAttr

# from kelp.models.project_config import ProjectConfig
from kelp.models.abac import AbacPolicy
from kelp.models.function import KelpFunction
from kelp.models.metric_view import MetricView
from kelp.models.source import Source
from kelp.models.table import Table

# from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive
# from kelp.utils.jinja_parser import load_yaml_with_jinja


logger = logging.getLogger(f"{__name__}")


class Catalog(BaseModel):
    """Catalog of tables and metric views defined in the kelp project.

    Maintains indexed collections of tables and metric views with deduplication
    of names (keeps first occurrence). Provides efficient lookup by name.

    Attributes:
        models: List of Table definitions in the catalog.
        metric_views: List of MetricView definitions in the catalog.
        sources: List of Source definitions in the catalog.
    """

    models: list[Table] = Field(
        default_factory=list,
        description="Table definitions in the catalog",
    )
    metric_views: list[MetricView] = Field(
        default_factory=list,
        description="Metric view definitions in the catalog",
    )
    functions: list[KelpFunction] = Field(
        default_factory=list,
        description="Function definitions in the catalog",
    )
    abacs: list[AbacPolicy] = Field(
        default_factory=list,
        description="ABAC policy definitions in the catalog",
    )
    sources: list[Source] = Field(
        default_factory=list,
        description="Source definitions in the catalog",
    )
    _table_index_cache: dict[str, Table] = PrivateAttr(default_factory=dict)
    _metrics_index_cache: dict[str, MetricView] = PrivateAttr(default_factory=dict)
    _function_index_cache: dict[str, KelpFunction] = PrivateAttr(default_factory=dict)
    _abac_index_cache: dict[str, AbacPolicy] = PrivateAttr(default_factory=dict)
    _source_index_cache: dict[str, Source] = PrivateAttr(default_factory=dict)
    _table_index_built: bool = PrivateAttr(default=False)
    _metrics_index_built: bool = PrivateAttr(default=False)
    _function_index_built: bool = PrivateAttr(default=False)
    _abac_index_built: bool = PrivateAttr(default=False)
    _source_index_built: bool = PrivateAttr(default=False)

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

    def _build_function_index(self) -> None:
        """Build name -> KelpFunction index and record duplicates."""
        index: dict[str, KelpFunction] = {}

        for function in self.functions:
            name = getattr(function, "name", None) or "<unknown>"
            if name in index:
                logger.warning(
                    "Duplicate function name encountered: %s (kept first occurrence)",
                    name,
                )
                continue
            index[name] = function

        self._function_index_cache = index
        self._function_index_built = True

    def _build_abac_index(self) -> None:
        """Build name -> AbacPolicy index and record duplicates."""
        index: dict[str, AbacPolicy] = {}

        for abac in self.abacs:
            name = getattr(abac, "name", None) or "<unknown>"
            if name in index:
                logger.warning(
                    "Duplicate ABAC policy name encountered: %s (kept first occurrence)",
                    name,
                )
                continue
            index[name] = abac

        self._abac_index_cache = index
        self._abac_index_built = True

    def _build_source_index(self) -> None:
        """Build name -> Source index and record duplicates."""
        index: dict[str, Source] = {}

        for source in self.sources:
            name = getattr(source, "name", None) or "<unknown>"
            if name in index:
                logger.warning(
                    "Duplicate source name encountered: %s (kept first occurrence)",
                    name,
                )
                continue
            index[name] = source

        self._source_index_cache = index
        self._source_index_built = True

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

    @property
    def function_index(self) -> dict[str, KelpFunction]:
        """Return a mapping name -> KelpFunction (keeps first occurrence on dupes)."""
        if not self._function_index_built:
            self._build_function_index()
        return self._function_index_cache

    @property
    def abac_index(self) -> dict[str, AbacPolicy]:
        """Return a mapping name -> AbacPolicy (keeps first occurrence on dupes)."""
        if not self._abac_index_built:
            self._build_abac_index()
        return self._abac_index_cache

    @property
    def source_index(self) -> dict[str, Source]:
        """Return a mapping name -> Source (keeps first occurrence on dupes)."""
        if not self._source_index_built:
            self._build_source_index()
        return self._source_index_cache

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

    def get_function(self, name: str) -> KelpFunction:
        """Return the first KelpFunction matching `name`."""
        function = self.function_index.get(name)
        if function is None:
            raise KeyError(f"Function not found in catalog: {name}")
        return function

    def get_abac(self, name: str) -> AbacPolicy:
        """Return the first AbacPolicy matching `name`."""
        abac = self.abac_index.get(name)
        if abac is None:
            raise KeyError(f"ABAC policy not found in catalog: {name}")
        return abac

    def get_source(self, name: str) -> Source:
        """Return the first Source matching `name`."""
        source = self.source_index.get(name)
        if source is None:
            raise KeyError(f"Source not found in catalog: {name}")
        return source

    def get_tables(self) -> list[Table]:
        """Return all Tables in the catalog as a list."""
        return self.models

    def get_metric_views(self) -> list[MetricView]:
        """Return all MetricViews in the catalog as a list."""
        return self.metric_views

    def get_functions(self) -> list[KelpFunction]:
        """Return all Functions in the catalog as a list."""
        return self.functions

    def get_abacs(self) -> list[AbacPolicy]:
        """Return all ABAC policies in the catalog as a list."""
        return self.abacs

    def get_sources(self) -> list[Source]:
        """Return all Sources in the catalog as a list."""
        return self.sources

    def refresh_index(self) -> None:
        """Rebuild the internal indices from `self.models` and `self.metric_views`.

        Call this if `self.models` or `self.metric_views` has been modified after construction.
        """
        self._table_index_cache = {}
        self._metrics_index_cache = {}
        self._function_index_cache = {}
        self._abac_index_cache = {}
        self._source_index_cache = {}
        self._table_index_built = False
        self._metrics_index_built = False
        self._function_index_built = False
        self._abac_index_built = False
        self._source_index_built = False
        self._build_table_index()
        self._build_metrics_index()
        self._build_function_index()
        self._build_abac_index()
        self._build_source_index()
