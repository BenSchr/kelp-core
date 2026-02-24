"""Global runtime context lifecycle management.

This module manages the lifecycle of the global RuntimeContext, which represents
the complete configuration and state for a single execution context.

Key features:
- Thread-safe singleton pattern for global context storage
- Automatic context initialization with configuration override support
- Context refresh capability for multi-scenario testing
- Integrated logging configuration

The context can be initialized through the init() function and retrieved
via get_context() or directly through ContextStore methods.
"""

import logging
import threading

from kelp.config.runtime import load_runtime_config
from kelp.models.runtime_context import RuntimeContext
from kelp.utils.logging import configure_logging


class ContextExistsError(Exception):
    """Raised when attempting to set a global context while one already exists."""


class ContextMissingError(Exception):
    """Raised when a requested context is missing."""


class ContextStore:
    """Thread-safe store for managing the global runtime context."""

    _lock = threading.Lock()
    _state: RuntimeContext | None = None

    @classmethod
    def get(cls) -> RuntimeContext | None:
        """Get the current global context without creating one."""
        with cls._lock:
            return cls._state

    @classmethod
    def set(cls, ctx: RuntimeContext, overwrite: bool = False) -> None:
        """Set the global context.

        Args:
            ctx: RuntimeContext to store globally.
            overwrite: If False, raises ContextExistsError if context already exists.
                      If True, replaces existing context.

        Raises:
            ContextExistsError: If context exists and overwrite=False.

        """
        with cls._lock:
            if cls._state is not None and not overwrite:
                raise ContextExistsError(
                    "A global context already exists. Use overwrite=True to replace it.",
                )
            cls._state = ctx

    @classmethod
    def clear(cls) -> None:
        """Clear the global context."""
        with cls._lock:
            cls._state = None

    @classmethod
    def get_or_create(
        cls,
        project_file_path: str | None = None,
        target: str | None = None,
        overwrite_vars: dict | None = None,
        refresh: bool = False,
        store_in_global: bool = True,
    ) -> RuntimeContext:
        """Get or create the global runtime Context.

        If a global Context already exists, it is returned unless `refresh` is True,
        in which case a new Context is created and stored globally.

        Args:
            project_file_path: Path to the project file. If None, auto-detected.
            target: Target name to use (e.g., 'dev', 'prod').
            overwrite_vars: Variables to override.
            refresh: If True, creates new context even if one exists globally.
            store_in_global: If True, stores the context globally.

        Returns:
            RuntimeContext instance (either stored or newly created).

        """
        with cls._lock:
            if cls._state is not None and not refresh:
                return cls._state
        # configure logging if not already configured through init
        configure_logging()

        ctx = load_runtime_config(project_file_path, target, overwrite_vars)
        with cls._lock:
            if store_in_global and (cls._state is None or refresh):
                cls._state = ctx
            if cls._state is not None and not refresh:
                ctx = cls._state
        logger = logging.getLogger(__name__)
        logger.debug(
            "Runtime context initialized with project_file_path=%s, target=%s, overwrite_vars=%s",
            ctx.project_config.project_file_path,
            target,
            overwrite_vars,
        )
        logger.debug("Resolved runtime variables: %s", ctx.runtime_vars)

        return ctx


def init(
    project_root: str | None = None,
    target: str | None = None,
    overwrite_vars: dict | None = None,
    *,
    refresh: bool = False,
    store_in_global: bool = True,
    log_level: str | None = None,
) -> RuntimeContext:
    """Initialize and return a runtime Context for the given project root.

    If `store_in_global` is True, the created Context is stored
    globally for later retrieval.

    If `refresh` is True, any existing global Context is overwritten.

    Args:
        project_root: Path to the project root. If None, auto-detected.
        target: Target name to use (e.g., 'dev', 'prod').
        overwrite_vars: Variables to override.
        refresh: If True, creates new context even if one exists globally.
        store_in_global: If True, stores the context globally.
        log_level: Log level to configure.

    Returns:
        RuntimeContext instance.

    Raises:
        ContextExistsError: If a global context already exists
            and refresh is False.

    """
    if log_level:
        configure_logging(log_level)
    return ContextStore.get_or_create(
        project_root,
        target,
        overwrite_vars,
        refresh=refresh,
        store_in_global=store_in_global,
    )


def get_context(init: bool = True) -> RuntimeContext:
    """Return the stored global RuntimeContext.

    Args:
        init: If True, initializes context if not already set. If False, returns None if not set.

    Returns:
        RuntimeContext instance.

    Raises:
        ContextMissingError: If no context is set and init=False, or after initialization if still missing.

    """
    ctx = ContextStore.get_or_create() if init else ContextStore.get()
    if ctx is None:
        raise ContextMissingError("No global context is set. Please initialize one first.")
    return ctx


def set_var(key: str, value) -> None:
    """Set a runtime variable in the global context."""
    ctx = get_context()
    if ctx.runtime_vars is None:
        ctx.runtime_vars = {}
    ctx.runtime_vars[key] = value


def get_var(key: str, default=None):
    """Get a runtime variable from the global context."""
    ctx = get_context()
    if ctx.runtime_vars is None:
        return default
    return ctx.runtime_vars.get(key, default)
