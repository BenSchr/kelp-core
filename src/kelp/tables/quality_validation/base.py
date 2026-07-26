from importlib.util import find_spec

from kelp.config import project_settings


def quality_monitoring_target() -> str | None:
    """Return the DQX monitoring table FQN, or None when monitoring is disabled or unset.

    Reads the project context once.

    Returns:
        The fully qualified name of the DQX monitoring table, or None when
        monitoring is switched off or no table is configured.
    """
    quality_config = project_settings().quality_config
    if not quality_config.dqx_monitoring_enabled:
        return None
    return quality_config.dqx_monitoring_fqn or None


def ensure_dqx_installed() -> None:
    """Verify that the optional ``databricks-labs-dqx`` dependency is importable.

    Raises:
        ImportError: If the package is not installed.
    """
    try:
        if not find_spec("databricks.labs.dqx"):
            raise ImportError(
                "The databricks-labs-dqx package is required for quality monitoring features. "
                "Please install it with `pip install databricks-labs-dqx`."
            )
    except ModuleNotFoundError:
        raise ImportError(  # noqa: B904
            "The databricks-labs-dqx package is required for quality monitoring features. "
            "Please install it with `pip install databricks-labs-dqx`."
        )
