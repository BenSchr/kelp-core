"""Model metadata accessor functions backed by :class:`~kelp.service.model_manager.ModelManager`.

Every function in this module delegates to
:meth:`ModelManager.build_model` which returns a lightweight
:class:`~kelp.service.model_manager.KelpModel` — no SDP expectations or
pipeline-specific logic is involved.
"""

from kelp.models.model import Column
from kelp.service.model_manager import KelpModel, ModelManager


def get_model(name: str) -> KelpModel:
    """Get the KelpModel object for a given model name."""
    return ModelManager.build_model(name)


def ref(name: str) -> str:
    """Get the fully qualified name for a model."""
    return ModelManager.build_model(name).fqn or name


def schema(name: str, exclude: list[str] | None = None) -> str | None:
    """Get the Spark schema DDL for a model.

    Args:
        name: Model name.
        exclude: Column names to exclude from the schema.

    Returns:
        Spark schema DDL string, or ``None`` if not available.
    """
    return ModelManager.build_model(name, exclude=exclude).schema


def schema_lite(name: str, exclude: list[str] | None = None) -> str | None:
    """Get the raw Spark schema without constraints or generated columns.

    Args:
        name: Model name.
        exclude: Column names to exclude from the schema.

    Returns:
        Spark schema DDL string, or ``None`` if not available.
    """
    return ModelManager.build_model(name, exclude=exclude).schema_lite


def ddl(name: str, if_not_exists: bool = True) -> str | None:
    """Get the full CREATE TABLE DDL statement for a model."""
    return ModelManager.build_model(name).get_ddl(if_not_exists=if_not_exists)


def columns(name: str) -> list[Column]:
    """Get the column definitions for a model."""
    model = ModelManager.build_model(name)
    if model.root_model:
        return model.root_model.columns
    return []


def func(name: str) -> str:
    """Get the fully qualified name for a Unity Catalog function."""
    from kelp.config import get_context

    context = get_context()
    return context.catalog_index.get("functions", name).get_qualified_name()


def source(name: str) -> str:
    """Get the path for a data source."""
    from kelp.service.source_manager import SourceManager

    return SourceManager.get_path(name)


def source_options(name: str) -> dict:
    """Get the options dictionary for a data source."""
    from kelp.service.source_manager import SourceManager

    return SourceManager.get_options(name)
