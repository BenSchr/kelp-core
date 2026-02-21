import logging

from kelp.config.settings import create_settings_resolver


def configure_logging(level: str | None = None) -> None:
    ## Don't set basicConfig just for own namespace

    logger = logging.getLogger("kelp")

    # Stop duplication in environments that configure root (e.g., Jupyter/IPython).
    logger.propagate = False

    if not level:
        level = create_settings_resolver().resolve("log_level", default=None)

    if level:
        mapped = logging.getLevelNamesMapping().get(level.upper())
        if mapped is None:
            raise ValueError(f"Unknown logging level: {level}")
        logger.setLevel(mapped)

    # Idempotent: only add one real stream handler
    has_streamhandler = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    if not has_streamhandler:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(h)
