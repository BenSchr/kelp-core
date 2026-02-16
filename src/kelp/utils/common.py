from importlib.util import find_spec


def require_optional(pkg: str, extra: str | None = None) -> None:
    if find_spec(pkg) is None:
        hint = f"[{extra}]" if extra else ""
        raise ImportError(
            f"Optional dependency '{pkg}' is not installed.\nInstall through {__package__}{hint}\n"
        )
