from pathlib import Path


def find_path_by_name(
    start_path: str | Path,
    target_name: str,
    search_strategy: str = "both",
    max_depth_up: int = 3,
    max_depth_down: int = 3,
) -> Path | None:
    """Find a file or directory by name using configurable search strategy.

    Searches for a target file or directory starting from start_path, with options
    to search parent directories (upward), child directories (downward), or both.

    Args:
        start_path: Starting path for the search. If path doesn't exist, uses current working directory.
        target_name: Name of file or directory to find.
        search_strategy: Search direction. Options: "up" (parents only), "down" (children only), "both" (default).
        max_depth_up: Maximum levels to traverse upward from start_path (default: 3).
        max_depth_down: Maximum levels to traverse downward from start_path (default: 3).

    Returns:
        Path object if found, None otherwise.

    Example:
        >>> find_path_by_name(".", "kelp_project.yml", search_strategy="both")
        Path('/path/to/kelp_project.yml')

    """
    start_path = Path(start_path).resolve()

    # Search upward (parents)
    if search_strategy in ("up", "both"):
        current = start_path
        for _ in range(max_depth_up):
            candidate = current / target_name
            if candidate.exists():
                return candidate
            parent = current.parent
            if parent == current:  # Reached filesystem root
                break
            current = parent

    # Search downward (children)
    if search_strategy in ("down", "both"):

        def _search_recursive(path: Path, depth: int) -> Path | None:
            """Recursively search for target_name in child directories."""
            if depth <= 0:
                return None

            try:
                for item in path.iterdir():
                    if item.name == target_name:
                        return item
                    if item.is_dir():
                        result = _search_recursive(item, depth - 1)
                        if result:
                            return result
            except (PermissionError, OSError):
                # Skip directories we can't read
                pass

            return None

        result = _search_recursive(start_path, max_depth_down)
        if result:
            return result

    return None
