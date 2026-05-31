"""Custom datacontract Importer that converts kelp Models to a data contract.

From the datacontract library's perspective, this is an "importer"
(importing from kelp format into a data contract). From kelp's perspective,
this is an "exporter" (exporting kelp metadata to data contract format).

Usage:
    from datacontract.imports.importer_factory import importer_factory
    from kelp.integration.odcs.custom_importer import KelpImporter

    importer_factory.register_importer("kelp", KelpImporter)

    data_contract = DataContract()
    result = data_contract.import_from_source(format="kelp", source=kelp_yaml_str)
"""

import logging
from typing import Any

from datacontract.imports.importer import Importer
from datacontract.imports.odcs_helper import (
    create_odcs,
    create_property,
    create_schema_object,
    create_server,
    map_sql_type_to_logical,
)
from open_data_contract_standard.model import (
    CustomProperty,
    DataQuality,
    OpenDataContractStandard,
    Relationship,
    SchemaObject,
    SchemaProperty,
)

from kelp.models.model import Model

logger = logging.getLogger(__name__)


class KelpImporter(Importer):
    """Custom datacontract Importer that reads kelp metadata.

    Subclasses datacontract.imports.importer.Importer.
    Register with importer_factory.register_importer("kelp", KelpImporter).
    """

    def import_source(  # ty: ignore[invalid-method-override]
        self,
        source: Model,
        import_args: dict,
    ) -> OpenDataContractStandard:
        """Import kelp metadata into an OpenDataContractStandard.

        Args:
            source: `Model` to convert.
            import_args: Additional import arguments. Supports:
                - name (str): Contract name override.
                - include_server (bool): Include ODCS server using model catalog/schema.

        Returns:
            An OpenDataContractStandard instance.
        """
        model = _parse_model_from_input(source)

        contract_name = import_args.get("name")
        include_server = bool(import_args.get("include_server", False))
        return kelp_model_to_datacontract(
            model=model,
            contract_name=contract_name,
            include_server=include_server,
        )


def _parse_model_from_input(source: Model) -> Model:
    """Parse single Kelp model from model object.

    Args:
        source: Model.

    Returns:
        Parsed model.
    """
    if isinstance(source, Model):
        return source

    raise ValueError("KelpImporter only accepts a single Model as input")


def _meta_value(model: Model, key: str) -> str | None:
    value = model.meta.get(key)
    if value is None:
        return None
    return str(value)


def _to_custom_properties(values: dict[str, Any]) -> list[CustomProperty] | None:
    filtered = {k: v for k, v in values.items() if v is not None and v != ""}
    if not filtered:
        return None
    return [CustomProperty(property=k, value=v) for k, v in filtered.items()]


def _encode_tags_for_odcs(tags: dict[str, str]) -> list[str] | None:
    if not tags:
        return None
    encoded = [f"{key}:{value}" if value else key for key, value in tags.items()]
    return encoded or None


def _quality_to_odcs(quality: Any) -> list[DataQuality] | None:
    if quality is None:
        return None

    checks: list[DataQuality] = []

    if getattr(quality, "engine", None) == "dqx":
        dqx_checks = [check for check in getattr(quality, "checks", []) if isinstance(check, dict)]
        checks.extend(
            [
                DataQuality(
                    type="custom",
                    engine="dqx",
                    name=check.get("name") if isinstance(check.get("name"), str) else None,
                    implementation={k: v for k, v in check.items() if k != "engine"},
                )
                for check in dqx_checks
            ]
        )
        return checks or None

    if getattr(quality, "engine", None) == "sdp":
        sdp_impl = quality.model_dump(exclude_none=True, exclude_defaults=True)
        sdp_impl.pop("engine", None)
        checks.append(
            DataQuality(
                type="custom",
                engine="sdp",
                name="kelp_sdp_quality",
                implementation=sdp_impl,
            )
        )
        return checks or None

    logger.warning("Unsupported quality engine '%s' during Kelp->ODCS export", quality.engine)
    return None


def _model_to_schema_object(model: Model) -> SchemaObject:
    """Convert a Kelp model to an ODCS SchemaObject."""
    partition_positions = {column: index + 1 for index, column in enumerate(model.partition_cols)}

    primary_key_columns: list[str] = []
    foreign_key_constraints: list[Any] = []
    for constraint in model.constraints:
        if constraint.type == "primary_key":
            primary_key_columns = list(constraint.columns)
        elif constraint.type == "foreign_key":
            foreign_key_constraints.append(constraint)

    primary_key_position_map = {
        column: index + 1 for index, column in enumerate(primary_key_columns)
    }

    properties: list[SchemaProperty] = []
    properties_by_name: dict[str, SchemaProperty] = {}
    for column in model.columns:
        logical_type = map_sql_type_to_logical(column.data_type)  # ty: ignore[invalid-argument-type]
        is_primary = column.name in primary_key_position_map
        property_obj = create_property(
            name=column.name,
            logical_type=logical_type,
            physical_type=column.data_type,  # ty: ignore[invalid-argument-type]
            description=column.description,  # ty: ignore[invalid-argument-type]
            required=(not column.nullable),
            primary_key=is_primary or None,  # ty: ignore[invalid-argument-type]
            primary_key_position=primary_key_position_map.get(column.name),  # ty: ignore[invalid-argument-type]
            tags=_encode_tags_for_odcs(column.tags),  # ty: ignore[invalid-argument-type]
        )

        if column.name in partition_positions:
            property_obj.partitioned = True
            property_obj.partitionKeyPosition = partition_positions[column.name]

        properties.append(property_obj)
        properties_by_name[column.name] = property_obj

    schema_relationships: list[Relationship] = []
    for fk in foreign_key_constraints:
        if len(fk.columns) == 1 and len(fk.reference_columns) == 1:
            local_col = fk.columns[0]
            property_obj = properties_by_name.get(local_col)
            if property_obj is not None:
                if property_obj.relationships is None:
                    property_obj.relationships = []
                property_obj.relationships.append(
                    Relationship(
                        type="foreignKey",
                        to=f"{fk.reference_table}.{fk.reference_columns[0]}",
                    )
                )
                continue

        from_cols = [f"{model.name}.{column}" for column in fk.columns]
        to_cols = [f"{fk.reference_table}.{column}" for column in fk.reference_columns]
        schema_relationships.append(
            Relationship(
                type="foreignKey",
                from_=from_cols,
                to=to_cols,
            )
        )

    schema = create_schema_object(
        name=model.name,
        description=model.description,  # ty: ignore[invalid-argument-type]
        properties=properties,
        tags=_encode_tags_for_odcs(model.tags),  # ty: ignore[invalid-argument-type]
    )

    if schema_relationships:
        schema.relationships = schema_relationships

    quality = _quality_to_odcs(model.quality)
    if quality:
        schema.quality = quality

    schema_meta = {
        key: value
        for key, value in model.meta.items()
        if key not in {"domain", "status", "owner", "odcs_id", "odcs_version"}
    }
    schema_custom = _to_custom_properties(schema_meta)
    if schema_custom:
        schema.customProperties = schema_custom

    return schema


def kelp_model_to_datacontract(
    model: Model,
    contract_name: str | None = None,
    include_server: bool = False,
) -> OpenDataContractStandard:
    """Convert a single Kelp model to an ODCS data contract."""
    domain = _meta_value(model, "odcs_domain")
    status = _meta_value(model, "odcs_status")
    odcs_id = _meta_value(model, "odcs_id")
    odcs_version = _meta_value(model, "odcs_version")
    owner = _meta_value(model, "odcs_owner")

    odcs = create_odcs(
        id=odcs_id,  # ty: ignore[invalid-argument-type]
        name=contract_name or model.name,
        version=odcs_version or "1.0.0",
        status=status or "draft",
    )
    if domain:
        odcs.domain = domain

    contract_custom_meta = {
        key: value
        for key, value in model.meta.items()
        if key not in {"odcs_domain", "odcs_status", "odcs_id", "odcs_version", "odcs_owner"}
    }
    if owner and "owner" not in contract_custom_meta:
        contract_custom_meta["owner"] = owner

    custom_properties = _to_custom_properties(contract_custom_meta)
    if custom_properties:
        odcs.customProperties = custom_properties

    odcs.schema_ = [_model_to_schema_object(model)]

    if include_server and model.catalog and model.schema_:
        odcs.servers = [
            create_server(
                name="databricks",
                server_type="databricks",
                database=model.catalog,
                schema=model.schema_,
                catalog=model.catalog,
            )
        ]

    return odcs


def export_to_contract(model: Model) -> OpenDataContractStandard:
    """Backward-compatible wrapper for older call sites."""
    return kelp_model_to_datacontract(model=model)
