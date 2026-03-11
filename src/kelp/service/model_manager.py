from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kelp.config import get_context
from kelp.meta.context import MetaRuntimeContext
from kelp.models.model import (
    Column,
    DQXQuality,
    ForeignKeyConstraint,
    GeneratedExpressionColumnConfig,
    GeneratedIdentityColumnConfig,
    Model,
    PrimaryKeyConstraint,
    SDPQuality,
)
from kelp.models.project_config import ProjectConfig


@dataclass
class KelpModel:
    name: str
    table_type: str | None = None
    comment: str | None = None
    table_properties: dict | None = None
    spark_conf: dict | None = None
    path: str | None = None
    partition_cols: list[str] | None = None
    cluster_by_auto: bool | None = None
    cluster_by: list[str] | None = None
    row_filter: str | None = None
    fqn: str | None = None
    schema: str | None = None
    schema_lite: str | None = None
    dqx_checks: list[dict] | None = None

    validation_table: str | None = None
    quarantine_table: str | None = None
    target_table: str | None = None
    root_model: Model | None = None

    def get_dqx_check_obj(self) -> DQXQuality | None:
        if self.root_model and isinstance(self.root_model.quality, DQXQuality):
            return self.root_model.quality
        return None

    def get_ddl(self, if_not_exists: bool = True) -> str | None:
        mapped_type = _UC_TYPE.get(self.table_type.lower(), "TABLE") if self.table_type else "TABLE"
        return (
            ModelManager.get_spark_schema_ddl(
                self.root_model,
                table_type=mapped_type,
                if_not_exists=if_not_exists,
            )
            if self.root_model
            else None
        )


@dataclass
class KelpSdpModel(KelpModel):
    expect_all: dict | None = None
    expect_all_or_fail: dict | None = None
    expect_all_or_drop: dict | None = None
    expect_all_or_quarantine: dict | None = None

    def params(self, exclude: list[str] | None = None) -> dict[str, str]:
        exclude = exclude or []
        default_exclude = [
            "expect_all",
            "expect_all_or_drop",
            "expect_all_or_fail",
            "expect_all_or_quarantine",
        ]
        exclude = list(set(exclude) | set(default_exclude))
        return self.get_sdp_params(exclude=exclude)

    def params_raw(self, exclude: list[str] | None = None) -> dict[str, str]:
        exclude = exclude or []
        return self.get_sdp_params(exclude=exclude)

    def params_cst(self, exclude: list[str] | None = None) -> dict[str, str]:
        exclude = exclude or []
        default_exclude = ["expect_all_or_quarantine"]
        exclude = list(set(exclude) | set(default_exclude))
        return self.get_sdp_params(exclude=exclude)

    def get_sdp_params(self, exclude: list[str] | None = None) -> dict[str, Any]:
        exclude = exclude or []
        params = {
            "name": self.fqn,
            "comment": self.comment,
            "spark_conf": self.spark_conf,
            "table_properties": self.table_properties,
            "path": self.path,
            "partition_cols": self.partition_cols,
            "cluster_by_auto": self.cluster_by_auto,
            "cluster_by": self.cluster_by,
            "schema": self.schema or None,
            "row_filter": self.row_filter,
            "expect_all": self.expect_all,
            "expect_all_or_drop": self.expect_all_or_drop,
            "expect_all_or_fail": self.expect_all_or_fail,
            "expect_all_or_quarantine": self.expect_all_or_quarantine,
        }
        return {k: v for k, v in params.items() if (v is not None or "") and k not in exclude}

    def get_ddl(self, if_not_exists: bool = False, or_refresh: bool = True) -> str | None:
        mapped_type = _UC_TYPE.get(self.table_type.lower(), "TABLE") if self.table_type else "TABLE"
        return (
            ModelManager.get_spark_schema_ddl(
                self.root_model,
                table_type=mapped_type,
                if_not_exists=if_not_exists,
                or_refresh=or_refresh,
            )
            if self.root_model
            else None
        )


def _get_project_config(ctx: MetaRuntimeContext) -> ProjectConfig:
    return ctx.project_settings


def _get_model_from_context(
    ctx: MetaRuntimeContext,
    model_name: str,
    *,
    soft_handle: bool,
) -> Model | None:
    try:
        return ctx.catalog_index.get("models", model_name)
    except KeyError:
        if soft_handle:
            return None
        raise


class ModelManager:
    @classmethod
    def get_spark_schema(
        cls,
        model: Model,
        include_constraints: bool = False,
        add_generated: bool = False,
    ) -> str:
        builder = SparkSchemaBuilder(model).add_columns(add_generated=add_generated)
        if include_constraints:
            builder = builder.add_constraints()
        return builder.build_raw()

    @classmethod
    def get_spark_schema_ddl(
        cls,
        model: Model,
        table_type: str = "Table",
        if_not_exists: bool = False,
        or_refresh: bool = False,
    ) -> str | None:
        builder = SparkSchemaBuilder(model)
        builder.add_columns(
            add_generated=True
        ).add_constraints().add_clustering().add_table_properties()
        return builder.build_ddl(
            table_type=table_type, if_not_exists=if_not_exists, or_refresh=or_refresh
        )

    @classmethod
    def build_validation_model_name(
        cls,
        ctx: MetaRuntimeContext,
        model_name: str,
        schema: str | None = None,
        catalog: str | None = None,
    ) -> str:
        project_config = _get_project_config(ctx)
        prefix = project_config.quarantine_config.validation_prefix
        suffix = project_config.quarantine_config.validation_suffix
        validation_model_name = f"{prefix}{model_name}{suffix}"
        return cls.build_qualified_model_name(catalog, schema, validation_model_name)

    @classmethod
    def build_quarantine_model_name(
        cls,
        ctx: MetaRuntimeContext,
        model_name: str,
        schema: str | None = None,
        catalog: str | None = None,
    ) -> str:
        project_config = _get_project_config(ctx)
        prefix = project_config.quarantine_config.quarantine_prefix
        suffix = project_config.quarantine_config.quarantine_suffix
        quarantine_catalog = project_config.quarantine_config.quarantine_catalog
        quarantine_schema = project_config.quarantine_config.quarantine_schema
        qnt_name = f"{prefix}{model_name}{suffix}"
        schema = quarantine_schema or schema
        catalog = quarantine_catalog or catalog
        return cls.build_qualified_model_name(catalog, schema, qnt_name)

    @classmethod
    def build_qualified_model_name(cls, catalog: str | None, schema: str | None, name: str) -> str:
        parts = []
        if catalog:
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(name)
        return ".".join(parts)

    @classmethod
    def get_qualified_name_from_model(
        cls,
        model: Model,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        parts = []
        catalog = catalog or model.catalog
        schema = schema or model.schema_
        if catalog:
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(model.name)
        return ".".join(parts)

    @classmethod
    def build_sdp_model(
        cls,
        model_name: str,
        model: Model | None = None,
        ctx: MetaRuntimeContext | None = None,
        soft_handle: bool = False,
    ) -> KelpSdpModel:
        ctx = ctx or get_context()
        if model is None:
            model = _get_model_from_context(ctx, model_name, soft_handle=soft_handle)
            if model is None:
                model = Model(name=model_name)
        if model is None:
            raise KeyError(f"Model not found in catalog: {model_name}")

        sdp_model = KelpSdpModel(name=model.name)
        sdp_model.comment = model.description
        sdp_model.spark_conf = model.spark_conf
        sdp_model.table_properties = model.table_properties
        sdp_model.path = model.path
        sdp_model.partition_cols = model.partition_cols
        sdp_model.cluster_by_auto = model.cluster_by_auto
        sdp_model.cluster_by = model.cluster_by
        sdp_model.row_filter = model.row_filter
        sdp_model.fqn = cls.get_qualified_name_from_model(model)
        sdp_model.schema = cls.get_spark_schema(model, include_constraints=True, add_generated=True)
        sdp_model.schema_lite = cls.get_spark_schema(
            model, include_constraints=False, add_generated=False
        )

        if model.quality and isinstance(model.quality, SDPQuality):
            sdp_model.expect_all = model.quality.expect_all
            sdp_model.expect_all_or_drop = model.quality.expect_all_or_drop
            sdp_model.expect_all_or_fail = model.quality.expect_all_or_fail
            sdp_model.expect_all_or_quarantine = model.quality.expect_all_or_quarantine
        elif model.quality and isinstance(model.quality, DQXQuality):
            sdp_model.dqx_checks = model.quality.checks

        sdp_model.validation_table = cls.build_validation_model_name(
            ctx, model.name, model.schema_, model.catalog
        )
        sdp_model.quarantine_table = cls.build_quarantine_model_name(
            ctx, model.name, model.schema_, model.catalog
        )
        sdp_model.target_table = (
            sdp_model.validation_table if sdp_model.expect_all_or_quarantine else sdp_model.fqn
        )
        sdp_model.root_model = model
        return sdp_model

    @classmethod
    def build_model(
        cls,
        model_name: str,
        model: Model | None = None,
        ctx: MetaRuntimeContext | None = None,
    ) -> KelpModel:
        ctx = ctx or get_context()
        model = model or _get_model_from_context(ctx, model_name, soft_handle=False)
        if model is None:
            raise KeyError(f"Model not found in catalog: {model_name}")

        kelp_model = KelpModel(name=model.name)
        kelp_model.comment = model.description
        kelp_model.spark_conf = model.spark_conf
        kelp_model.table_properties = model.table_properties
        kelp_model.path = model.path
        kelp_model.partition_cols = model.partition_cols
        kelp_model.cluster_by_auto = model.cluster_by_auto
        kelp_model.cluster_by = model.cluster_by
        kelp_model.row_filter = model.row_filter
        kelp_model.fqn = cls.get_qualified_name_from_model(model)
        kelp_model.schema = cls.get_spark_schema(
            model, include_constraints=True, add_generated=True
        )
        kelp_model.schema_lite = cls.get_spark_schema(
            model, include_constraints=False, add_generated=False
        )
        kelp_model.root_model = model

        if model.quality and isinstance(model.quality, DQXQuality):
            kelp_model.dqx_checks = model.quality.checks

        return kelp_model


_UC_TYPE: dict[str, str] = {
    "managed": "TABLE",
    "view": "VIEW",
    "materialized_view": "MATERIALIZED VIEW",
    "streaming_table": "STREAMING TABLE",
}


class SparkSchemaBuilder:
    def __init__(self, model: Model):
        self.model = model
        self.table_parts: list[str] = []
        self.outer_parts: list[str] = []

    def add_columns(self, add_generated: bool = False) -> SparkSchemaBuilder:
        for col in self.model.columns:
            self.table_parts.append(self._column_to_string(col, add_generated=add_generated))
        return self

    def add_constraints(self) -> SparkSchemaBuilder:
        for constraint in self.model.constraints:
            if isinstance(constraint, PrimaryKeyConstraint):
                cols = ", ".join(constraint.columns)
                self.table_parts.append(f"CONSTRAINT {constraint.name} PRIMARY KEY ({cols})")
            elif isinstance(constraint, ForeignKeyConstraint):
                cols = ", ".join(constraint.columns)
                # Resolve FK reference_table to FQN if it exists in local catalog
                ref_table = constraint.reference_table
                if "." not in ref_table:
                    # Try to resolve unqualified name to FQN from context
                    try:
                        ctx = get_context(init=False)
                        ref_model = ctx.catalog_index.get("models", ref_table)
                        if (
                            ref_model
                            and hasattr(ref_model, "catalog")
                            and hasattr(ref_model, "schema_")
                        ):
                            catalog = ref_model.catalog
                            schema = ref_model.schema_
                            if catalog and schema:
                                ref_table = f"{catalog}.{schema}.{ref_table}"
                    except (KeyError, RuntimeError):
                        pass  # If not found or no context initialized, keep as is
                self.table_parts.append(
                    f"CONSTRAINT {constraint.name} FOREIGN KEY ({cols}) REFERENCES {ref_table} ({', '.join(constraint.reference_columns)})"
                )
        return self

    def add_clustering(self) -> SparkSchemaBuilder:
        if self.model.cluster_by_auto:
            self.outer_parts.append("CLUSTERED BY (AUTO)")
        elif self.model.cluster_by:
            self.outer_parts.append(f"CLUSTERED BY ({', '.join(self.model.cluster_by)})")
        elif self.model.partition_cols:
            self.outer_parts.append(f"PARTITIONED BY ({', '.join(self.model.partition_cols)})")
        return self

    def add_table_properties(self) -> SparkSchemaBuilder:
        if self.model.table_properties:
            props = ", ".join(f"'{k}'='{v}'" for k, v in self.model.table_properties.items())
            self.outer_parts.append(f"TBLPROPERTIES ({props})")
        return self

    def build_raw(self) -> str:
        return ", ".join(self.table_parts)

    def build_ddl(
        self, table_type: str = "Table", if_not_exists: bool = False, or_refresh: bool = False
    ) -> str:
        table_schema = ",\n".join(self.table_parts)
        ddl = "CREATE "
        if or_refresh:
            ddl += "OR REFRESH "
        ddl += f"{table_type} "
        if if_not_exists:
            ddl += "IF NOT EXISTS "
        ddl += f"{self.model.get_qualified_name()} (\n{table_schema}\n)"
        if self.outer_parts:
            ddl += "\n" + "\n".join(self.outer_parts)
        return ddl

    def _column_to_string(self, column: Column, add_generated: bool = False) -> str:
        col_str = f"{column.name} {column.data_type}"
        if not column.nullable:
            col_str += " NOT NULL"
        if column.generated and add_generated:
            if column.generated.type == "identity":
                gen = column.generated
                identity_str = "GENERATED "
                if isinstance(gen, GeneratedIdentityColumnConfig):
                    identity_str += "AS DEFAULT " if gen.as_default else "AS ALWAYS "
                    identity_str += (
                        f"IDENTITY (START WITH {gen.start_with} INCREMENT BY {gen.increment_by})"
                    )
                col_str += f" {identity_str}"
            elif column.generated.type == "expression":
                gen = column.generated
                if isinstance(gen, GeneratedExpressionColumnConfig):
                    col_str += f" GENERATED ALWAYS AS ({gen.expression})"
        if column.description:
            col_str += f" COMMENT '{column.description}'"
        return col_str
