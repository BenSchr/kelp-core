from importlib.util import find_spec

from kelp.config import project_settings


def get_quality_monitorng_table_name() -> str | None:
    project_config = project_settings()
    return project_config.quality_config.dqx_monitoring_fqn


def should_apply_quality_monitoring() -> bool:
    project_config = project_settings()
    return project_config.quality_config.dqx_monitoring_enabled and bool(
        project_config.quality_config.dqx_monitoring_fqn
    )


def ensure_dqx_installed():
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
