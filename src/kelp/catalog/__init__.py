"""Unity Catalog sync adapter (v2).

Public surface:
- Domain models: ``Table``, ``Column``, ``Constraint``
- Diff types: ``TableDiff`` and related diff dataclasses
- Services: ``TableDiffCalculator`` and ``UCQueryBuilder``
- Orchestrator: ``UnityCatalogAdapter``
"""

from kelp.catalog.api import sync_catalog, sync_tables, sync_metric_views, create_metric_views

__all__ = ["sync_catalog", "create_metric_views", "sync_metric_views", "sync_tables"]
