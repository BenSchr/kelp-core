"""Custom datacontract Exporter that converts a data contract to kelp Models.

From the datacontract library's perspective, this is an 'exporter'
(exporting the contract to kelp format). From kelp's perspective, this is
an 'importer' (importing a data contract into kelp metadata).

Usage:
    from datacontract.export.exporter_factory import exporter_factory
    from kelp.integration.odcs.custom_exporter import KelpExporter

    exporter_factory.register_exporter("kelp", KelpExporter)

    data_contract = DataContract(data_contract_file="contract.yml")
    result = data_contract.export(export_format="kelp")
"""

import logging
from typing import Any

from datacontract.export.exporter import Exporter
from datacontract.export.sql_type_converter import convert_to_databricks
from open_data_contract_standard.model import OpenDataContractStandard, SchemaObject

from kelp.models.model import Model

logger = logging.getLogger(__name__)


def _decode_odcs_tags(tags: list[str] | None) -> dict[str, str]:
    decoded: dict[str, str] = {}
    if not tags:
        return decoded
    for tag in tags:
        if ":" in tag:
            key, value = tag.split(":", 1)
            decoded[key] = value
        else:
            decoded[tag] = ""
    return decoded


def _custom_properties_to_dict(custom_properties: list | None) -> dict[str, str]:
    values: dict[str, str] = {}
    if not custom_properties:
        return values
    for prop in custom_properties:
        key = getattr(prop, "property", None)
        value = getattr(prop, "value", None)
        if key:
            values[str(key)] = "" if value is None else str(value)
    return values


class KelpExporter(Exporter):
    """Custom datacontract Exporter that produces kelp metadata YAML.

    Subclasses datacontract.export.exporter.Exporter.
    Register with exporter_factory.register_exporter("kelp", KelpExporter).
    """

    def export(  # ty: ignore[invalid-method-override]
        self,
        data_contract: OpenDataContractStandard,
        schema_name: str,
        server: str,
        sql_server_type: str,
        export_args: dict,
    ) -> list[Model]:
        """Export a data contract to kelp metadata YAML format.

        Args:
            data_contract: An OpenDataContractStandard instance.
            schema_name: The schema to export ('all' for all).
            server: Server name (unused).
            sql_server_type: SQL server type (unused).
            export_args: Additional export arguments. Supports:
                - generate_dqx_rules (bool): Generate DQX quality rules.
                - data_contract_file (str): Path to contract file (required for DQX).

        Returns:
            YAML string of kelp metadata.
        """

        result_dict_list: list[dict[str, Any]] = _convert_to_kelp_dict(data_contract)

        if export_args.get("generate_dqx_rules"):
            dqx_rules = _generate_dqx_rules(data_contract)
            if dqx_rules:
                result_dict_list = _add_dqx_rules_to_models(result_dict_list, dqx_rules)

        result_models: list[Model] = [Model(**model_dict) for model_dict in result_dict_list]

        return result_models


def _convert_to_kelp_dict(data_contract: OpenDataContractStandard) -> list[dict]:
    """Convert the first schema object of a data contract to a kelp Model dict."""
    schema = data_contract.schema_
    if not schema:
        raise ValueError("Data contract has no schema objects")
    result = [_schema_obj_to_dict(data_contract, s) for s in schema if s.name or s.physicalName]
    return result


def _schema_obj_to_dict(
    data_contract: OpenDataContractStandard,
    schema_obj: SchemaObject,
) -> dict:
    """Convert a single ODCS schema object to a kelp Model dict.

    Args:
        data_contract: The parent data contract (for domain/status tags).
        schema_obj: The ODCS SchemaObject to convert.

    Returns:
        Dict suitable for constructing a kelp Model.
    """
    result: dict = {}

    # Name: prefer physicalName, fallback to name
    result["name"] = schema_obj.name or schema_obj.physicalName or ""

    # Description
    if schema_obj.description:
        result["description"] = schema_obj.description

    # Tags: only explicit schema-level tags
    tags: dict[str, str] = _decode_odcs_tags(schema_obj.tags)
    if tags:
        result["tags"] = tags

    # Meta: contract-level ODCS fields are stored with odcs_ prefix
    meta: dict[str, str] = {}
    if data_contract.domain:
        meta["odcs_domain"] = data_contract.domain
    if data_contract.status:
        meta["odcs_status"] = data_contract.status
    if data_contract.id:
        meta["odcs_id"] = data_contract.id
    if data_contract.version:
        meta["odcs_version"] = data_contract.version

    contract_custom = _custom_properties_to_dict(data_contract.customProperties)
    excluded_meta_keys = {"odcs_domain", "odcs_status", "odcs_id", "odcs_version", "odcs_owner"}
    meta.update(
        {key: value for key, value in contract_custom.items() if key not in excluded_meta_keys}
    )

    # Owner can be exposed directly on contract or derived from team members
    owner = getattr(data_contract, "owner", None)
    if not owner:
        team = getattr(data_contract, "team", None)
        members = getattr(team, "members", None) if team else None
        if members:
            owner_usernames = []
            for member in members:
                role = getattr(member, "role", None)
                if isinstance(role, str) and role.lower() == "owner":
                    username = getattr(member, "username", None)
                    if username:
                        owner_usernames.append(username)
            if owner_usernames:
                owner = ",".join(owner_usernames)
    if not owner:
        owner = contract_custom.get("owner")

    if owner:
        meta["odcs_owner"] = owner

    schema_custom = _custom_properties_to_dict(schema_obj.customProperties)
    meta.update(
        {key: value for key, value in schema_custom.items() if key not in excluded_meta_keys}
    )

    if meta:
        result["meta"] = meta

    # Columns and primary key tracking
    columns: list[dict] = []
    pk_cols: list[tuple[int, str]] = []

    if schema_obj.properties:
        for prop in schema_obj.properties:
            col_name = prop.physicalName or prop.name or ""
            col: dict = {"name": col_name}

            # Type: use physicalType (lowercased) directly; fall back to
            # convert_to_databricks for logical-type-only properties.
            if prop.physicalType:
                col["data_type"] = prop.physicalType.lower()
            elif prop.logicalType:
                converted = convert_to_databricks(prop)
                if converted:
                    col["data_type"] = converted.lower()

            # Nullability derived from required flag
            if prop.required is not None:
                col["nullable"] = not prop.required

            if prop.description:
                col["description"] = prop.description

            if prop.tags:
                col["tags"] = _decode_odcs_tags(prop.tags)

            columns.append(col)

            if prop.primaryKey:
                pos = prop.primaryKeyPosition if prop.primaryKeyPosition is not None else 0
                pk_cols.append((pos, col_name))

    if columns:
        result["columns"] = columns

    # Constraints
    constraints: list[dict] = []

    if pk_cols:
        pk_cols.sort(key=lambda x: x[0])
        constraints.append(
            {
                "type": "primary_key",
                "name": f"pk_{result['name']}",
                "columns": [c[1] for c in pk_cols],
            }
        )

    # Schema-level foreign key relationships
    if schema_obj.relationships:
        for rel in schema_obj.relationships:
            if rel.type == "foreignKey":
                from_list = (
                    rel.from_ if isinstance(rel.from_, list) else ([rel.from_] if rel.from_ else [])
                )
                to_list = rel.to if isinstance(rel.to, list) else ([rel.to] if rel.to else [])
                local_cols = [c.split(".")[-1] for c in from_list]
                ref_parts = [c.rsplit(".", 1) for c in to_list]
                if ref_parts:
                    ref_table = ref_parts[0][0] if len(ref_parts[0]) > 1 else ""
                    ref_cols = [p[-1] for p in ref_parts]
                    constraints.append(
                        {
                            "type": "foreign_key",
                            "name": f"fk_{result['name']}_{ref_table}",
                            "columns": local_cols,
                            "reference_table": ref_table,
                            "reference_columns": ref_cols,
                        }
                    )

    # Property-level foreign key relationships
    if schema_obj.properties:
        for prop in schema_obj.properties:
            col_name = prop.physicalName or prop.name or ""
            if prop.relationships:
                for rel in prop.relationships:
                    if rel.type == "foreignKey":
                        to = (
                            rel.to
                            if isinstance(rel.to, str)
                            else (rel.to[0] if isinstance(rel.to, list) and rel.to else "")
                        )
                        if to:
                            parts = to.rsplit(".", 1)
                            ref_table = parts[0] if len(parts) > 1 else ""
                            ref_col = parts[-1]
                            constraints.append(
                                {
                                    "type": "foreign_key",
                                    "name": f"fk_{col_name}_{ref_table}",
                                    "columns": [col_name],
                                    "reference_table": ref_table,
                                    "reference_columns": [ref_col],
                                }
                            )

    if constraints:
        result["constraints"] = constraints

    # Partition columns sorted by partitionKeyPosition
    if schema_obj.properties:
        partitioned = [
            (p.partitionKeyPosition or 0, p.physicalName or p.name or "")
            for p in schema_obj.properties
            if p.partitioned
        ]
        if partitioned:
            partitioned.sort(key=lambda x: x[0])
            result["partition_cols"] = [c[1] for c in partitioned]

    # Quality: collect DQX custom checks from property-level then schema-level,
    # and preserve SDP implementation payloads from schema-level quality checks.
    dqx_checks: list[dict] = []
    sdp_quality_impl: dict | None = None
    if schema_obj.properties:
        for prop in schema_obj.properties:
            if prop.quality:
                for dq in prop.quality:
                    if (
                        dq.type == "custom"
                        and dq.engine == "dqx"
                        and isinstance(dq.implementation, dict)
                    ):
                        dqx_checks.extend([dq.implementation])

    if schema_obj.quality:
        for dq in schema_obj.quality:
            if dq.type == "custom" and dq.engine == "dqx" and isinstance(dq.implementation, dict):
                dqx_checks.extend([{k: v for k, v in dq.implementation.items() if k != "engine"}])
            if (
                dq.type == "custom"
                and dq.engine == "sdp"
                and isinstance(dq.implementation, dict)
                and sdp_quality_impl is None
            ):
                sdp_quality_impl = dq.implementation

    if dqx_checks:
        result["quality"] = {"engine": "dqx", "checks": dqx_checks}
    elif sdp_quality_impl:
        sdp_copy = {k: v for k, v in sdp_quality_impl.items() if k != "engine"}
        sdp_copy["engine"] = "sdp"
        result["quality"] = sdp_copy

    return result


def datacontract_to_kelp_models(data_contract: OpenDataContractStandard) -> list[Model]:
    """Convert all schema objects in a data contract to kelp Models.

    Args:
        data_contract: An OpenDataContractStandard instance.

    Returns:
        List of kelp Model instances, one per schema object.
    """
    return [
        Model(**_schema_obj_to_dict(data_contract, schema_obj))
        for schema_obj in (data_contract.schema_ or [])
    ]


def _add_dqx_rules_to_models(model_list: list[dict], dqx_rules: list[dict]) -> list[dict]:
    for model_dict in model_list:
        if dqx_rules:
            quality = model_dict.get("quality")
            quality_engine = quality.get("engine") if isinstance(quality, dict) else None
            if quality_engine is not None and quality_engine != "dqx":
                logger.warning(
                    "Model %s does not have DQX quality engine defined. Skipping adding DQX rules to this model.",
                    model_dict.get("name", "unknown"),
                )
                continue

            rules_for_model = [
                check
                for check in dqx_rules
                if check.get("user_metadata", {}).get("schema") == model_dict["name"]
            ]

            # remove user_metadata from rules since it's only needed for matching rules to models, not for the actual checks
            for r in rules_for_model:
                if "user_metadata" in r:
                    del r["user_metadata"]

            if rules_for_model:
                quality_dict = model_dict.get("quality")
                if not isinstance(quality_dict, dict):
                    quality_dict = {"engine": "dqx", "checks": []}
                    model_dict["quality"] = quality_dict

                checks_list = quality_dict.get("checks")
                if not isinstance(checks_list, list):
                    checks_list = []
                    quality_dict["checks"] = checks_list

                existing_names = {
                    check.get("name") for check in checks_list if isinstance(check, dict)
                }
                for rule in rules_for_model:
                    rule_name = rule.get("name") if isinstance(rule, dict) else None
                    if rule_name and rule_name in existing_names:
                        continue
                    checks_list.append(rule)
                    if rule_name:
                        existing_names.add(rule_name)
    return model_list


def _generate_dqx_rules(data_contract: OpenDataContractStandard) -> list[dict]:
    """Generate DQX rules from a data contract file.

    Requires `databricks-labs-dqx[datacontract]` to be installed.

    Args:
        data_contract_file: Path to the data contract YAML file.

    Returns:
        List of DQX rule dictionaries.

    Raises:
        ImportError: If required packages are not installed.

    """
    import sys
    from unittest.mock import MagicMock, Mock

    from datacontract.data_contract import DataContract

    # Mock the DQX LLM module due to known bug
    # https://github.com/databrickslabs/dqx/issues/1168
    class _FakeDQLLMEngine:
        pass

    llm_mock = Mock()
    llm_engine_mock = Mock()
    llm_engine_mock.DQLLMEngine = _FakeDQLLMEngine
    sys.modules["databricks.labs.dqx.llm"] = llm_mock
    sys.modules["databricks.labs.dqx.llm.llm_engine"] = llm_engine_mock

    try:
        from databricks.labs.dqx.profiler.generator import DQGenerator
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise ImportError(
            "'databricks-labs-dqx[datacontract]' and 'pyspark' is required for DQX rule generation. "
            "Install with: pip install 'databricks-labs-dqx[datacontract]' 'pyspark'"
        ) from e

    contract = DataContract(data_contract_str=data_contract.to_yaml())
    ws = MagicMock(spec=WorkspaceClient)
    generator = DQGenerator(workspace_client=ws)
    rules: list[dict] = generator.generate_rules_from_contract(
        contract=contract,
        generate_predefined_rules=False,  # Already have predefined rules in the contract, so skip generating them again
        process_text_rules=False,
    )
    return rules
