# from pathlib import Path

import logging
from pydantic import BaseModel, PrivateAttr, Field

# from kelp.models.project_config import ProjectConfig
from kelp.models.table import Table

# from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive
# from kelp.utils.jinja_parser import load_yaml_with_jinja


logger = logging.getLogger(f"{__name__}")


class Catalog(BaseModel):
    """Catalog of tables defined in the kelp project."""

    models: list[Table] = Field(default_factory=list)
    _index_cache: dict[str, Table] | None = PrivateAttr(default=None)

    # --- Index helpers -------------------------------------------------
    def _build_index(self) -> None:
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
        self._index_cache = index

    @property
    def index(self) -> dict[str, Table]:
        """Return a mapping name -> Table (keeps first occurrence on dupes)."""
        if self._index_cache is None:
            self._build_index()
        return self._index_cache

    def get_table(self, name: str, soft_handle: bool = False) -> Table:
        """Return the first Table matching `name` or None if not found."""
        table = self.index.get(name)
        if not table and not soft_handle:
            raise KeyError(f"Table not found in catalog: {name}")
        if not table and soft_handle:
            logger.warning(
                f"Table not found in catalog: {name}. Returning placeholder table since soft_handle=True."
            )
            table = Table(name=name)
        return table

    def get_tables(self) -> list[Table]:
        """Return all Tables in the catalog as a list."""
        return self.models

    def refresh_index(self) -> None:
        """Rebuild the internal index from `self.models`.

        Call this if `self.models` has been modified after construction.
        """
        self._index_cache = None
        self._build_index()
