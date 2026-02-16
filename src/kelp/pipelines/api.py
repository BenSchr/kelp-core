from kelp.service.table_manager import SDPTable, TableManager


def get_table(name: str) -> SDPTable:
    """Returns the SDPTable object for the given table name."""
    return TableManager.build_sdp_table(name)


## Use SDPTable object for api
def target(name: str) -> str:
    """returns the target name for the table if quarantine or validation is enabled, else returns the main table name"""
    return TableManager.build_sdp_table(name).target_table


def ref(name: str) -> str:
    """returns the source name for the table, which is always the main table name"""
    return TableManager.build_sdp_table(name).fqn


def schema(name: str) -> str | None:
    return TableManager.build_sdp_table(name).schema


def schema_lite(name: str) -> str | None:
    return TableManager.build_sdp_table(name).schema_lite


def params(name: str, exclude: list[str] = []) -> dict[str, str]:
    """
    Returns the streaming table parameters as a dictionary.

    Args:
        name (str): Table name.
        exclude (list[str], optional): List of parameters to exclude from the returned dictionary.

    Returns:
        dict[str, str]: Dictionary of streaming table parameters.
    """
    return TableManager.build_sdp_table(name).params(exclude=exclude)


def params_cst(name: str, exclude: list[str] = []) -> dict[str, str]:
    """returns the create streaming table parameters as a dictionary"""
    return TableManager.build_sdp_table(name).params_cst(exclude=exclude)


# def target(name: str) -> str:
#     """returns the target name for the table if quarantine or validation is enabled, else returns the main table name"""
#     table_manager = TableManager(name)
#     return table_manager.get_target()


# def ref(name: str) -> str:
#     """returns the source name for the table, which is always the main table name"""
#     table_manager = TableManager(name)
#     return table_manager.get_qualitified_table_name()


# def schema(name: str) -> str | None:
#     """Returns the raw Spark schema for Struct operations.
#     For additional constraints and generated columns, use `schema_full`."""
#     table_manager = TableManager(name)
#     return table_manager.get_spark_schema()


# def schema_full(name: str) -> str | None:
#     """returns the Spark schema for the table, including any constraints"""
#     table_manager = TableManager(name)
#     return table_manager.get_spark_schema(include_constraints=True, add_generated=True)


# def params(name: str, exclude: list[str] = []) -> dict[str, str]:
#     """
#     Returns the streaming table parameters as a dictionary.

#     Args:
#         name (str): Table name.
#         exclude (list[str], optional): List of parameters to exclude from the returned dictionary.

#     Returns:
#         dict[str, str]: Dictionary of streaming table parameters.
#     """
#     table_manager = TableManager(name)
#     default_exclude = [
#         "expect_all",
#         "expect_all_or_drop",
#         "expect_all_or_fail",
#         "expect_all_or_quarantine",
#     ]
#     exclude = list(set(exclude) | set(default_exclude))
#     return table_manager.get_sdp_table_params_as_dict(exclude=exclude)


# def params_cst(name: str, exclude: list[str] = []) -> dict[str, str]:
#     """returns the create streaming table parameters as a dictionary"""
#     table_manager = TableManager(name)
#     default_exclude = [
#         "expect_all_or_quarantine",
#     ]
#     exclude = list(set(exclude) | set(default_exclude))
#     return table_manager.get_sdp_table_params_as_dict(exclude=exclude)
