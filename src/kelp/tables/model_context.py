import logging
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from kelp.models.model_config import ModelConfig
from kelp.service.model_manager import KelpModel

logger = logging.getLogger(__name__)


@dataclass
class ModelContext:
    """Execution context injected into model functions at call time.

    Provides access to the Spark session, the resolved
    :class:`~kelp.service.model_manager.KelpModel` for the target table, and
    helpers for controlling how the result is materialized.

    Args:
        spark: Active SparkSession.
        this: The resolved KelpModel for the target table.
        full_refresh: When ``True``, always perform a full recompute.
        target_exists: Whether the target table already exists in the catalog.
    """

    spark: SparkSession
    this: KelpModel
    full_refresh: bool
    target_exists: bool
    model_config: ModelConfig | None = field(default=None, init=False)

    def config(self, **kwargs: object) -> ModelConfig:
        """Get or update the active model configuration.

        Call with no arguments to read the current config.

        Call with keyword arguments to patch individual fields on the current
        config — only the supplied keys are changed, everything else is kept:

        .. code-block:: python

            ctx.config(write_mode="overwrite")
            ctx.config(merge_condition="source.id = target.id")

        Args:
            **kwargs: :class:`~kelp.models.model_config.ModelConfig` field
                overrides to merge into the current configuration.

        Returns:
            The active :class:`~kelp.models.model_config.ModelConfig` instance.
        """
        if kwargs:
            current = self.model_config.model_dump() if self.model_config is not None else {}
            self.model_config = ModelConfig(**{**current, **kwargs})  # type: ignore[arg-type]
        return self.model_config if self.model_config is not None else ModelConfig()

    def is_incremental(self) -> bool:
        """Return ``True`` when the target exists and a full refresh is not requested."""
        return self.target_exists and not self.full_refresh

