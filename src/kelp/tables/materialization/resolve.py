"""Name and config resolution for a materialization.

Materialization works with or without kelp metadata, and the shape of the name
decides which of the two applies:

- **Unqualified** (``orders``) is a kelp model reference. The model supplies the
  target FQN, the CREATE TABLE DDL, DQX quality checks and catalog metadata sync.
  A name that resolves to no model is an error: writing to an unqualified table in
  whatever the session's default catalog and schema happen to be is a wrong-table
  write, not a fallback.
- **Qualified** (``catalog.schema.orders``) is a target table. No metadata is
  looked up at all; the name and the passed config are used exactly as given.

So a job without kelp metadata is not a special mode — it simply names its targets
in full.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from kelp.models.model_mat_config import (
    MaterializationConfig,
    parse_materialization_config,
    resolve_config,
)

if TYPE_CHECKING:
    from kelp.models.model import DQXQuality
    from kelp.service.model_manager import KelpModel

logger = logging.getLogger(__name__)

#: How a full refresh resets the target before it is rebuilt.
#:
#: ``drop`` removes the table and lets the write path recreate it. ``replace``
#: keeps the table in place — re-applying the DDL as ``CREATE OR REPLACE TABLE``,
#: or truncating when no DDL is available — so Unity Catalog grants, tags and
#: table history survive the refresh.
FullRefreshStrategy = Literal["drop", "replace"]


@dataclass
class ResolvedMaterializationInputs:
    """Resolved runtime inputs for a materialization invocation."""

    target_name: str
    effective_config: MaterializationConfig
    create_table_ddl: str | None
    model_name: str
    kelp_model: "KelpModel | None"
    dqx_quality: "DQXQuality | None"
    replace_table_ddl: str | None = None


def split_identifier(table_name: str) -> list[str]:
    """Split a table name on the dots that separate namespaces.

    Dots inside backticks belong to a single identifier and do not split it.

    Args:
        table_name: Name to split.

    Returns:
        The name's parts, in order.
    """
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    for char in table_name:
        if char == "`":
            quoted = not quoted
            current.append(char)
        elif char == "." and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def is_qualified_name(table_name: str) -> bool:
    """Whether a name addresses a table directly instead of naming a kelp model.

    Args:
        table_name: Name to inspect.

    Returns:
        ``True`` for a qualified table name, ``False`` for a bare model name.
    """
    return len(split_identifier(table_name)) > 1


def quoted_identifier(table_name: str) -> str:
    """Backtick-quote a table name for interpolation into a SQL statement.

    Args:
        table_name: Table name, qualified or not, with or without backticks.

    Returns:
        The name with every part quoted, so a name from YAML or a decorator
        argument cannot change the shape of the statement it lands in.
    """
    parts = []
    for part in split_identifier(table_name):
        if part.startswith("`") and part.endswith("`") and len(part) > 1:
            parts.append(part)
        else:
            parts.append(f"`{part.replace('`', '``')}`")
    return ".".join(parts)


def _resolve_model(table_name: str) -> "KelpModel":
    """Resolve kelp metadata for an unqualified model name.

    Args:
        table_name: Unqualified kelp model name.

    Returns:
        The resolved model.

    Raises:
        LookupError: If no kelp model of that name can be resolved, either because
            no kelp project was found or because the project declares no such
            model. Both cases would otherwise write to an unqualified table in the
            session's default catalog and schema. Errors from a project that exists
            but cannot be loaded propagate unchanged.
    """
    from kelp.service.model_manager import ModelManager

    hint = (
        "An unqualified name is resolved against kelp metadata; pass a qualified table "
        f"name (e.g. '<catalog>.<schema>.{table_name}') to materialize without metadata."
    )

    try:
        model = ModelManager.build_model(table_name, soft_handle=True)
    except FileNotFoundError as e:
        raise LookupError(f"No kelp project found to resolve model '{table_name}'. {hint}") from e

    if model.root_model is None:
        raise LookupError(f"No kelp model named '{table_name}'. {hint}")
    return model


def _without_metadata(
    table_name: str,
    override: "MaterializationConfig | None",
) -> ResolvedMaterializationInputs:
    """Build inputs for a target that no kelp model backs."""
    return ResolvedMaterializationInputs(
        target_name=table_name,
        effective_config=resolve_config(None, override),
        create_table_ddl=None,
        replace_table_ddl=None,
        model_name=table_name,
        kelp_model=None,
        dqx_quality=None,
    )


def resolve_materialization_inputs(
    *,
    table_name: str,
    config: "MaterializationConfig | dict | None",
) -> ResolvedMaterializationInputs:
    """Resolve model metadata and config into a single explicit runtime object.

    Args:
        table_name: Unqualified kelp model name, or a qualified table name that
            bypasses metadata resolution entirely.
        config: Optional runtime override config.

    Returns:
        The resolved inputs, with metadata-derived fields left empty for a
        qualified ``table_name``.

    Raises:
        LookupError: If ``table_name`` is unqualified and resolves to no kelp model.
    """
    override = parse_materialization_config(config)

    if is_qualified_name(table_name):
        logger.debug(
            "'%s' is a qualified table name; skipping kelp metadata resolution.",
            table_name,
        )
        return _without_metadata(table_name, override)

    kelp_model = _resolve_model(table_name)
    return ResolvedMaterializationInputs(
        target_name=kelp_model.fqn or table_name,
        effective_config=resolve_config(kelp_model.materialization, override),
        create_table_ddl=kelp_model.get_ddl(if_not_exists=True),
        replace_table_ddl=kelp_model.get_replace_ddl(),
        model_name=kelp_model.name,
        kelp_model=kelp_model,
        dqx_quality=kelp_model.dqx_quality,
    )
