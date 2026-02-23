"""Unity Catalog sync adapter (v2).

Public surface:
- Domain models: ``Table``, ``Column``, ``Constraint``
- Diff types: ``TableDiff`` and related diff dataclasses
- Services: ``TableDiffCalculator`` and ``UCQueryBuilder``
- Orchestrator: ``UnityCatalogAdapter``
"""

from kelp.catalog.api import sync_catalog, sync_table

__all__ = ["sync_catalog", "sync_table"]
