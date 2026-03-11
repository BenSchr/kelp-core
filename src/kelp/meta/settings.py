"""Self-contained settings resolution for meta runtimes.

Priority for resolution (highest to lowest):
1. Passed by function argument (init_settings)
2. Spark configuration (if available and Spark is active)
3. Environment variables (OS)
4. Target configuration
5. Default settings
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _get_spark_session():
    """Get active Spark session if available, else None."""
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001
        return None


class SourceResolver:
    """Base class for resolving settings from a specific source."""

    def resolve(self, key: str, default: Any = None) -> Any:
        """Resolve a key from this source. Return None if not found."""
        raise NotImplementedError


class InitSource(SourceResolver):
    """Direct initialization arguments passed by caller."""

    def __init__(self, values: dict[str, Any]):
        self.values = values or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


class EnvSource(SourceResolver):
    """Environment variables from OS."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    def resolve(self, key: str, default: Any = None) -> Any:
        env_key = f"{self.prefix}{key.upper()}"
        value = os.getenv(env_key)
        return value if value is not None else default


class SparkSource(SourceResolver):
    """Spark configuration (auto-detected, optional)."""

    def __init__(self, spark_prefix: str):
        self.spark = _get_spark_session()
        self.spark_prefix = spark_prefix

    def is_available(self) -> bool:
        """Check if Spark is available and active."""
        return self.spark is not None

    def resolve(self, key: str, default: Any = None) -> Any:
        if not self.is_available():
            return default

        try:
            value = self.spark.conf.get(f"{self.spark_prefix}.{key}")
            return value if value is not None else default
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to resolve from Spark conf for key '%s': %s", key, exc)
            return default


class TargetSource(SourceResolver):
    """Target-specific configuration."""

    def __init__(self, target_config: dict[str, Any]):
        self.target_config = target_config or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.target_config.get(key, default)


class DefaultSource(SourceResolver):
    """Default settings."""

    def __init__(self, defaults: dict[str, Any]):
        self.defaults = defaults or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.defaults.get(key, default)


class SettingsResolver:
    """Resolve settings with configurable source priority."""

    def __init__(self, sources: list[SourceResolver]):
        self.sources = sources

    def resolve(self, key: str, default: Any = None) -> Any:
        """Resolve a key by checking sources in priority order."""
        for source in self.sources:
            value = source.resolve(key, None)
            if value is not None:
                logger.debug("Resolved '%s' from %s", key, source.__class__.__name__)
                return value
        logger.debug("Key '%s' not found in any source, using default: %s", key, default)
        return default


def create_settings_resolver(
    *,
    init_settings: dict[str, Any] | None = None,
    target_settings: dict[str, Any] | None = None,
    default_settings: dict[str, Any] | None = None,
    env_prefix: str,
    spark_prefix: str,
) -> SettingsResolver:
    """Create a SettingsResolver with standard priority order."""
    sources: list[SourceResolver] = [
        InitSource(init_settings or {}),
        SparkSource(spark_prefix=spark_prefix),
        EnvSource(prefix=env_prefix),
        TargetSource(target_settings or {}),
        DefaultSource(default_settings or {}),
    ]
    return SettingsResolver(sources)


def resolve_setting(
    *,
    key: str,
    default: Any = None,
    init_settings: dict[str, Any] | None = None,
    target_settings: dict[str, Any] | None = None,
    default_settings: dict[str, Any] | None = None,
    env_prefix: str,
    spark_prefix: str,
) -> Any:
    """Resolve one setting using the standard resolver priority."""
    resolver = create_settings_resolver(
        init_settings=init_settings,
        target_settings=target_settings,
        default_settings=default_settings,
        env_prefix=env_prefix,
        spark_prefix=spark_prefix,
    )
    return resolver.resolve(key, default)
