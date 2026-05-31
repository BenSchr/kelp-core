"""Generic model metadata API for use in any Spark job."""

from kelp.config import init
from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.tables.api import (
    columns,
    ddl,
    func,
    get_model,
    ref,
    schema,
    schema_lite,
    source,
    source_options,
)
from kelp.tables.materialization import materialize, materialized
from kelp.tables.model_context import ModelContext

__all__ = [
    "ModelContext",
    "ModelMaterializationConfig",
    "columns",
    "ddl",
    "func",
    "get_model",
    "init",
    "materialize",
    "materialized",
    "ref",
    "schema",
    "schema_lite",
    "source",
    "source_options",
]
