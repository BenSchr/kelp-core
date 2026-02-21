import logging
import threading
from kelp.models.runtime_context import RuntimeContext
from kelp.config.runtime import load_runtime_config
from kelp.utils.logging import configure_logging


class ContextExistsError(Exception):
    """Raised when attempting to set a global context while one already exists."""


class ContextMissingError(Exception):
    """Raised when a requested context is missing."""


class ContextStore:
    _lock = threading.Lock()
    ctx_state: RuntimeContext | None = None

    @classmethod
    def get(cls) -> RuntimeContext | None:
        with cls._lock:
            return cls.ctx_state

    @classmethod
    def set(cls, ctx: RuntimeContext, overwrite: bool = False) -> None:
        with cls._lock:
            if cls.ctx_state is not None and not overwrite:
                raise ContextExistsError(
                    "A global context already exists. Use overwrite=True to replace it."
                )
            cls.ctx_state = ctx

    @classmethod
    def clear(cls) -> None:
        with cls._lock:
            cls.ctx_state = None

    @classmethod
    def getOrCreate(
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
        """
        with cls._lock:
            if cls.ctx_state is not None and not refresh:
                return cls.ctx_state
        # configure logging if not already configured through init
        configure_logging()

        ctx = load_runtime_config(project_file_path, target, overwrite_vars)
        with cls._lock:
            if store_in_global and (cls.ctx_state is None or refresh):
                cls.ctx_state = ctx
            if cls.ctx_state is not None and not refresh:
                ctx = cls.ctx_state
        logger = logging.getLogger(__name__)
        logger.debug(
            "Runtime context initialized with project_file_path=%s, target=%s, overwrite_vars=%s",
            project_file_path,
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

    Raises:
        ContextExistsError: If a global context already exists and
            `refresh` is False.
    """
    if log_level:
        configure_logging(log_level)
    return ContextStore.getOrCreate(
        project_root,
        target,
        overwrite_vars,
        refresh=refresh,
        store_in_global=store_in_global,
    )


def get_context(init: bool = True) -> RuntimeContext:
    """Return the stored global RuntimeContext."""
    if init:
        ctx = ContextStore.getOrCreate()
    else:
        ctx = ContextStore.get()
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
