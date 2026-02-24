from kelp.service.table_manager import KelpSdpTable, TableManager


def get_table(name: str) -> KelpSdpTable:
    """Returns the SDPTable object for the given table name."""
    return TableManager.build_sdp_table(name)


## Use SDPTable object for api
def target(name: str) -> str:
    """Returns the target name for the table if quarantine or validation is enabled, else returns the main table name"""
    return TableManager.build_sdp_table(name).target_table


def ref(name: str) -> str:
    """Returns the source name for the table, which is always the main table name"""
    return TableManager.build_sdp_table(name).fqn


def schema(name: str) -> str | None:
    return TableManager.build_sdp_table(name).schema


def schema_lite(name: str) -> str | None:
    return TableManager.build_sdp_table(name).schema_lite


def params(name: str, exclude: list[str] | None = None) -> dict[str, str]:
    """Returns the streaming table parameters as a dictionary.

    Args:
        name (str): Table name.
        exclude (list[str], optional): List of parameters to exclude from the returned dictionary.

    Returns:
        dict[str, str]: Dictionary of streaming table parameters.

    """
    exclude = exclude or []
    return TableManager.build_sdp_table(name).params(exclude=exclude)


def params_cst(name: str, exclude: list[str] | None = None) -> dict[str, str]:
    """Returns the create streaming table parameters as a dictionary"""
    exclude = exclude or []
    return TableManager.build_sdp_table(name).params_cst(exclude=exclude)
