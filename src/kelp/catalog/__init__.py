"""Unity Catalog sync adapter (v2).

Public surface:
- Domain models: ``Table``, ``Column``, ``Constraint``
- Diff types: ``TableDiff`` and related diff dataclasses
- Services: ``TableDiffCalculator`` and ``UCQueryBuilder``
- Orchestrator: ``UnityCatalogAdapter``
"""

from kelp.catalog.api import create_metric_views, sync_catalog, sync_metric_views, sync_tables

__all__ = ["create_metric_views", "sync_catalog", "sync_metric_views", "sync_tables"]
