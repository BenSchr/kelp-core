"""Databricks-SDK-based remote fetchers (default engine).

Thin wrappers around the SDK-based functions in
:mod:`kelp.utils.databricks`, giving them a place in the
:class:`~kelp.catalog.remote_fetchers.factory.RemoteFetcherFactory` lookup
alongside the Spark engine. Works from anywhere with a configured
Databricks CLI profile or token — no active Spark session required.
"""

from kelp.models.grant import Grant
from kelp.models.metric_view import MetricView
from kelp.models.model import Model
from kelp.utils.databricks import (
    get_direct_grants_from_dbx_sdk,
    get_metric_view_from_dbx_sdk,
    get_table_from_dbx_sdk,
)


class SdkTableFetcher:
    """Fetch table metadata via the Databricks SDK (REST API)."""

    def fetch(self, fqn: str, profile: str | None = None) -> Model | None:
        """Return the table's current state, or ``None`` if it doesn't exist."""
        return get_table_from_dbx_sdk(fqn, profile=profile)


class SdkMetricViewFetcher:
    """Fetch metric view metadata via the Databricks SDK (REST API)."""

    def fetch(self, fqn: str, profile: str | None = None) -> MetricView | None:
        """Return the metric view's current state, or ``None`` if it doesn't exist."""
        return get_metric_view_from_dbx_sdk(fqn, profile=profile)


class SdkGrantFetcher:
    """Fetch direct Unity Catalog grants via the Databricks SDK client."""

    def fetch(
        self,
        securable_type: str,
        fqn: str,
        profile: str | None = None,
    ) -> list[Grant] | None:
        """Return direct grants, or ``None`` if the securable does not exist."""
        return get_direct_grants_from_dbx_sdk(securable_type, fqn, profile=profile)
