def merge_params(
    params: dict[str, any], meta_params: dict[str, any], kwargs: dict[str, any] = {}
) -> dict[str, any]:
    """Cleans up params by removing None values and merging with meta_params and kwargs."""
    if meta_params.get("name"):
        params["name"] = meta_params["name"]
    params = {k: v for k, v in params.items() if v is not None}
    meta_params = {k: v for k, v in meta_params.items() if v is not None}
    params = {**meta_params, **params, **kwargs}
    return {k: v for k, v in params.items() if v is not None}
