"""V2 configuration API backed by the generic meta loading module.

This module provides kelp's v2 runtime context API using the reusable
``kelp.meta`` backend with the ``MetaFramework`` pattern.
"""

from __future__ import annotations

import logging
from typing import Any

from kelp.config.catalog_spec import CATALOG_PARSE_SPECS
from kelp.constants import KELP_PROJECT_FILENAME, KELP_PROJECT_HEADER
from kelp.meta import MetaFramework, MetaObjectSpec, MetaProjectSpec
from kelp.meta.context import MetaRuntimeContext
from kelp.models.project_config import ProjectConfig
from kelp.utils.logging import configure_logging

logger = logging.getLogger(__name__)

# Build kelp framework specification
_KELP_OBJECT_SPECS = tuple(
    MetaObjectSpec(
        root_key=spec.root_key,
        project_config_key=spec.project_config_key,
        path_attr=spec.path_attr,
        catalog_attr=spec.catalog_attr,
        model_class=spec.model_class,
        model_label=spec.model_label,
        preprocess=spec.preprocess,
    )
    for spec in CATALOG_PARSE_SPECS
)

KELP_SPEC = MetaProjectSpec(
    framework_id="kelp",
    project_header=KELP_PROJECT_HEADER,
    project_settings_model=ProjectConfig,
    object_specs=_KELP_OBJECT_SPECS,
    project_filename=KELP_PROJECT_FILENAME,
    resolve_runtime_settings=True,
)


class KelpFramework(MetaFramework):
    """Kelp meta framework API wrapper."""

    spec = KELP_SPEC


def init(
    project_file_path: str | None = None,
    target: str | None = None,
    init_vars: dict[str, Any] | None = None,
    refresh: bool = False,
    store_in_global: bool = True,
    run_policy_checks: bool = False,
    log_level: str | None = None,
) -> MetaRuntimeContext:
    """Initialize kelp runtime context from current directory.

    When ``policy_config.enabled`` is True in the project settings, metadata
    governance policies are evaluated immediately after loading. Warn-severity
    violations are logged; error-severity violations raise a RuntimeError.

    Args:
        project_file_path: Path to project file or directory.
        target: Target environment name.
        init_vars: Runtime variable overrides.
        refresh: If True, recreate context even if one already exists.
        store_in_global: Whether to store context globally.
        log_level: Optional log level to configure.

    Returns:
        The initialized MetaRuntimeContext.
    """
    if log_level:
        configure_logging(log_level)

    ctx = KelpFramework.init(
        project_file_path=project_file_path,
        target=target,
        init_vars=init_vars,
        refresh=refresh,
        store_in_global=store_in_global,
    )

    _run_policy_checks(ctx, run_policy_checks)

    return ctx


def _run_policy_checks(ctx: MetaRuntimeContext, overwrite_value: bool | None = None) -> None:
    """Run policy checks on the loaded catalog if policies are enabled.

    Args:
        ctx: The initialized runtime context.
    """
    policy_config = ctx.project_settings.policy_config
    run_policy_flag = overwrite_value if overwrite_value is not None else policy_config.enabled
    if not run_policy_flag:
        return

    from kelp.models.model import Model
    from kelp.models.policy_definition import Policy
    from kelp.service.policy_manager import PolicyManager

    models: list[Model] = ctx.catalog_index.get_all("models")
    policies: list[Policy] = ctx.catalog_index.get_all("policies")
    manager = PolicyManager()
    if not policies:
        logger.debug("Policy checks enabled but no policy definitions were loaded.")
        return

    violations = manager.check_catalog(models, policies, fast_exit=policy_config.fast_exit)

    if violations:
        manager.log_violations(violations)
        manager.raise_if_errors(violations)
    else:
        logger.debug("Policy check passed: no violations found.")


def get_context(init: bool = True):
    """Get kelp runtime context with auto-init by default.

    Args:
        init: If True (default), auto-initialize from current directory
            when context doesn't exist yet.

    Returns:
        MetaRuntimeContext for kelp.

    Raises:
        RuntimeError: If context doesn't exist and init=False.
    """
    return KelpFramework.get_context(init=init)


def project_settings() -> ProjectConfig:
    """Return the typed ProjectConfig from the current kelp runtime context.

    Auto-initializes from the current directory if no context exists yet.

    Returns:
        The resolved ProjectConfig for the current kelp project.
    """
    return get_context().project_settings
