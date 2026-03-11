"""Modular settings resolution with configurable priority order.

For project-level settings (not variables - those use simple dict merging).

Priority for resolution (highest to lowest):
1. Passed by function argument (init_settings)
2. Spark configuration (if available and Spark is active)
3. Environment variables (OS)
4. Target configuration
5. Default settings
"""

import logging
import os
from typing import Any

from kelp.constants import KELP_ENV_PREFIX

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

    def __init__(self, values: dict):
        self.values = values or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


class EnvSource(SourceResolver):
    """Environment variables from OS."""

    def __init__(self, prefix: str = "KELP_"):
        self.prefix = prefix

    def resolve(self, key: str, default: Any = None) -> Any:
        env_key = f"{self.prefix}{key.upper()}"
        value = os.getenv(env_key)
        return value if value is not None else default


class SparkSource(SourceResolver):
    """Spark configuration (auto-detected, optional)."""

    def __init__(self):
        self.spark = _get_spark_session()

    def is_available(self) -> bool:
        """Check if Spark is available and active."""
        return self.spark is not None

    def resolve(self, key: str, default: Any = None) -> Any:
        if not self.is_available():
            return default

        try:
            # Try to get from Spark conf
            value = self.spark.conf.get(f"kelp.{key}")
            return value if value is not None else default
        except Exception as e:  # noqa: BLE001
            logger.debug("Failed to resolve from Spark conf for key '%s': %s", key, e)
            return default


class TargetSource(SourceResolver):
    """Target-specific configuration."""

    def __init__(self, target_config: dict):
        self.target_config = target_config or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.target_config.get(key, default)


class DefaultSource(SourceResolver):
    """Default settings."""

    def __init__(self, defaults: dict):
        self.defaults = defaults or {}

    def resolve(self, key: str, default: Any = None) -> Any:
        return self.defaults.get(key, default)


class SettingsResolver:
    """Resolves project settings with configurable priority order.

    For project-level settings (not variables - use simple dict merging for those).
    """

    def __init__(self, sources: list[SourceResolver]):
        """Initialize resolver with ordered sources (highest to lowest priority).

        Args:
            sources: List of SourceResolver instances in priority order.

        """
        self.sources = sources

    def resolve(self, key: str, default: Any = None) -> Any:
        """Resolve a key by checking sources in priority order.

        Returns the first non-None value found, or the default.
        """
        for source in self.sources:
            value = source.resolve(key, None)
            if value is not None:
                logger.debug("Resolved '%s' from %s", key, source.__class__.__name__)
                return value

        logger.debug("Key '%s' not found in any source, using default: %s", key, default)
        return default

    def resolve_dict(self, keys: list[str], defaults: dict | None = None) -> dict:
        """Resolve multiple keys into a dictionary.

        Args:
            keys: List of keys to resolve.
            defaults: Default values dict, keyed by key names.

        Returns:
            Dictionary with resolved values.

        """
        defaults = defaults or {}
        result = {}
        for key in keys:
            result[key] = self.resolve(key, defaults.get(key))
        return result


def create_settings_resolver(
    init_settings: dict | None = None,
    target_settings: dict | None = None,
    default_settings: dict | None = None,
    env_prefix: str = KELP_ENV_PREFIX,
) -> SettingsResolver:
    """Create a SettingsResolver with standard priority order for project settings.

    Priority: init_settings > spark > os env > target_settings > default_settings

    Spark is auto-detected if available and active.

    Args:
        init_settings: Settings passed directly by caller.
        target_settings: Target-specific settings.
        default_settings: Default values.
        env_prefix: Prefix for environment variables (default: "KELP_").

    Returns:
        SettingsResolver instance with sources in priority order.

    """
    sources: list[SourceResolver] = [
        InitSource(init_settings or {}),
        SparkSource(),  # Auto-detects Spark
        EnvSource(prefix=env_prefix),
        TargetSource(target_settings or {}),
        DefaultSource(default_settings or {}),
    ]
    return SettingsResolver(sources)


def resolve_setting(
    key: str,
    default: Any = None,
    init_settings: dict | None = None,
    target_settings: dict | None = None,
    default_settings: dict | None = None,
    env_prefix: str = KELP_ENV_PREFIX,
) -> Any:
    """Resolve a single setting using the standard resolver priority.

    Args:
        key: Setting key to resolve.
        default: Default value if not found.
        init_settings: Settings passed directly by caller.
        target_settings: Target-specific settings.
        default_settings: Default values.
        env_prefix: Prefix for environment variables (default: "KELP_").

    Returns:
        Resolved value for the key.

    """
    resolver = create_settings_resolver(
        init_settings=init_settings,
        target_settings=target_settings,
        default_settings=default_settings,
        env_prefix=env_prefix,
    )
    return resolver.resolve(key, default)


def resolve_settings(
    keys: list[str],
    defaults: dict | None = None,
    init_settings: dict | None = None,
    target_settings: dict | None = None,
    default_settings: dict | None = None,
    env_prefix: str = KELP_ENV_PREFIX,
) -> dict:
    """Resolve multiple settings into a dictionary.

    Args:
        keys: Setting keys to resolve.
        defaults: Default values keyed by setting.
        init_settings: Settings passed directly by caller.
        target_settings: Target-specific settings.
        default_settings: Default values.
        env_prefix: Prefix for environment variables (default: "KELP_").

    Returns:
        Dictionary with resolved values.

    """
    resolver = create_settings_resolver(
        init_settings=init_settings,
        target_settings=target_settings,
        default_settings=default_settings,
        env_prefix=env_prefix,
    )
    return resolver.resolve_dict(keys, defaults)
