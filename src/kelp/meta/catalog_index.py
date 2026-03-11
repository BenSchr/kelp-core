"""Generic metadata catalog with indexing and accessor methods."""

from __future__ import annotations

import logging
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MetaCatalog:
    """Generic indexed catalog for metadata objects.

    Provides name-based lookup and deduplication for any object types stored
    in a hierarchical dict structure. Maintains lazy-built indices per object type.

    Example:
        >>> catalog = MetaCatalog(
        ...     raw_data={
        ...         "models": [{"name": "customers", ...}, ...],
        ...         "metric_views": [{"name": "daily_orders", ...}, ...],
        ...     }
        ... )
        >>> table = catalog.get("models", "customers")
        >>> all_tables = catalog.get_all("models")
    """

    def __init__(self, raw_data: dict[str, list[Any]]):
        """Initialize catalog from raw payload.

        Args:
            raw_data: Dict keyed by object type (e.g., "models", "metric_views")
                with lists of objects as values.
        """
        self._raw_data = raw_data
        self._indices: dict[str, dict[str, Any]] = {}
        self._built: dict[str, bool] = {}

    def _build_index(self, catalog_key: str) -> None:
        """Build name -> object index for a catalog key.

        Policy: keep-first for duplicate names and log a warning.
        """
        if self._built.get(catalog_key):
            return

        index: dict[str, Any] = {}
        objects = self._raw_data.get(catalog_key, [])

        for obj in objects:
            name = (
                getattr(obj, "name", None)
                or (obj.get("name") if isinstance(obj, dict) else None)
                or "<unknown>"
            )
            if name in index:
                logger.warning(
                    "Duplicate %s name encountered: %s (kept first occurrence)",
                    catalog_key,
                    name,
                )
                continue
            index[name] = obj

        self._indices[catalog_key] = index
        self._built[catalog_key] = True

    def get(self, catalog_key: str, name: str) -> Any:
        """Get object by name from catalog.

        Args:
            catalog_key: Object type key (e.g., "models", "metric_views").
            name: Object name to lookup.

        Returns:
            The first object matching the name.

        Raises:
            KeyError: If object is not found.
        """
        if catalog_key not in self._built or not self._built[catalog_key]:
            self._build_index(catalog_key)

        index = self._indices.get(catalog_key, {})
        obj = index.get(name)

        if obj is None:
            raise KeyError(f"{catalog_key} not found in catalog: {name}")

        return obj

    def get_all(self, catalog_key: str) -> list[Any]:
        """Get all objects from a catalog.

        Args:
            catalog_key: Object type key (e.g., "models", "metric_views").

        Returns:
            List of all objects for this catalog key.
        """
        return self._raw_data.get(catalog_key, [])

    def get_index(self, catalog_key: str) -> dict[str, Any]:
        """Get name -> object index for a catalog key.

        Builds index lazily on first access.

        Args:
            catalog_key: Object type key.

        Returns:
            Dict mapping object names to objects.
        """
        if catalog_key not in self._built or not self._built[catalog_key]:
            self._build_index(catalog_key)

        return self._indices.get(catalog_key, {})

    def __getattr__(self, attr: str) -> list[Any]:
        """Provide attribute access to catalog keys.

        Example: catalog.models instead of catalog.get_all("models")

        Args:
            attr: Attribute name (should match a catalog key).

        Returns:
            List of objects for that catalog key.

        Raises:
            AttributeError: If key doesn't exist in catalog.
        """
        if attr.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

        if attr in self._raw_data:
            return self.get_all(attr)

        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{attr}'. "
            f"Available: {list(self._raw_data.keys())}"
        )

    def refresh_index(self, catalog_key: str | None = None) -> None:
        """Rebuild indices.

        Args:
            catalog_key: Specific key to rebuild, or None to rebuild all.
        """
        if catalog_key:
            self._built[catalog_key] = False
            self._build_index(catalog_key)
        else:
            for key in self._raw_data:
                self._built[key] = False
                self._build_index(key)

    def keys(self) -> list[str]:
        """Return all catalog keys."""
        return list(self._raw_data.keys())

    def __repr__(self) -> str:
        keys_str = ", ".join(self.keys())
        return f"MetaCatalog({keys_str})"
