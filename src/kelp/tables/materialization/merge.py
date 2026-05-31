from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel
from kelp.tables.materialization.append_overwrite import AppendOverwriteMaterializer
from kelp.tables.materialization.base import (
    build_null_safe_change_condition,
    ensure_table_created,
    table_exists,
)

logger = logging.getLogger(__name__)


class MergeMaterializer:
    """Delta merge materializer using DeltaMergeBuilder."""

    @classmethod
    def run(
        cls,
        *,
        spark: SparkSession,
        dataframe: DataFrame,
        target_name: str,
        config: ModelMaterializationConfig,
        kelp_model: KelpModel | None,
    ) -> None:
        """Materialize by merging source rows into target.

        Behavior:
        - Creates target via DDL when metadata model exists and table is missing.
        - Falls back to append for first load if no metadata model/DDL is available.
        - Excludes non-overlapping columns from matched update logic.
        - Uses null-safe comparison for default matched-condition handling.

        Args:
            spark: Active SparkSession.
            dataframe: Source DataFrame.
            target_name: Target table name/FQN.
            config: Effective materialization config.
            kelp_model: Resolved Kelp model (if found).
        """
        existed_before = table_exists(spark, target_name)
        ensure_table_created(spark, kelp_model, target_name)
        created_now = (not existed_before) and table_exists(spark, target_name)

        if not table_exists(spark, target_name):
            logger.warning(
                "Target '%s' does not exist and no model DDL is available. Falling back to append.",
                target_name,
            )
            AppendOverwriteMaterializer.run(
                spark=spark,
                dataframe=dataframe,
                target_name=target_name,
                config=ModelMaterializationConfig(write_mode="append", options=config.options),
                kelp_model=kelp_model,
            )
            return

        if created_now and not config.unique_keys:
            logger.warning(
                "Target '%s' was newly created and no unique_keys were configured. "
                "Using append for initial load.",
                target_name,
            )
            AppendOverwriteMaterializer.run(
                spark=spark,
                dataframe=dataframe,
                target_name=target_name,
                config=ModelMaterializationConfig(write_mode="append", options=config.options),
                kelp_model=kelp_model,
            )
            return

        if not config.unique_keys:
            raise ValueError("'unique_keys' must be set when write_mode='merge'.")

        source_alias = config.source_alias
        target_alias = config.target_alias

        source_df = dataframe

        target_dt = DeltaTable.forName(spark, target_name)
        target_df = target_dt.toDF()
        source_cols = {col.lower(): col for col in source_df.columns}
        target_cols = {col.lower(): col for col in target_df.columns}
        overlapping_cols = [source_cols[col] for col in source_cols if col in target_cols]

        key_cols = [source_cols[k.lower()] for k in config.unique_keys if k.lower() in source_cols]
        if not key_cols:
            raise ValueError(
                "None of the configured unique_keys are present in the source DataFrame for merge."
            )

        missing_keys = [k for k in config.unique_keys if k.lower() not in target_cols]
        if missing_keys:
            raise ValueError(
                "Configured unique_keys missing from target table for merge: "
                + ", ".join(missing_keys)
            )

        on_parts = [
            f"{source_alias}.`{source_cols[k.lower()]}` = {target_alias}.`{target_cols[k.lower()]}`"
            for k in config.unique_keys
            if k.lower() in source_cols and k.lower() in target_cols
        ]
        merge_condition = " AND ".join(on_parts)

        if config.predicates:
            merge_condition = f"({merge_condition}) AND ({config.predicates})"

        builder = target_dt.alias(target_alias).merge(
            source_df.alias(source_alias), merge_condition
        )

        update_candidate_cols = [
            col for col in overlapping_cols if col.lower() not in {k.lower() for k in key_cols}
        ]

        if config.matched_update_include_cols:
            include = {c.lower() for c in config.matched_update_include_cols}
            update_candidate_cols = [c for c in update_candidate_cols if c.lower() in include]
        if config.matched_update_exclude_cols:
            exclude = {c.lower() for c in config.matched_update_exclude_cols}
            update_candidate_cols = [c for c in update_candidate_cols if c.lower() not in exclude]

        matched_condition = config.matched_condition
        if matched_condition is None:
            condition_cols = list(update_candidate_cols)
            if config.matched_condition_include_cols:
                include = {c.lower() for c in config.matched_condition_include_cols}
                condition_cols = [c for c in condition_cols if c.lower() in include]
            if config.matched_condition_exclude_cols:
                exclude = {c.lower() for c in config.matched_condition_exclude_cols}
                condition_cols = [c for c in condition_cols if c.lower() not in exclude]
            matched_condition = build_null_safe_change_condition(
                source_alias=source_alias,
                target_alias=target_alias,
                columns=condition_cols,
            )

        update_map: dict[str, str | Column] = {
            f"`{col}`": f"{source_alias}.`{col}`" for col in update_candidate_cols
        }
        if update_map:
            if matched_condition:
                builder = builder.whenMatchedUpdate(condition=matched_condition, set=update_map)
            else:
                builder = builder.whenMatchedUpdate(set=update_map)

        if config.not_matched_condition:
            builder = builder.whenNotMatchedInsertAll(condition=config.not_matched_condition)
        else:
            builder = builder.whenNotMatchedInsertAll()

        if config.not_matched_by_source_action:
            action = config.not_matched_by_source_action.lower().strip()
            if action == "delete":
                if config.not_matched_by_source_condition:
                    builder = builder.whenNotMatchedBySourceDelete(
                        condition=config.not_matched_by_source_condition
                    )
                else:
                    builder = builder.whenNotMatchedBySourceDelete()
            else:
                logger.warning(
                    "Unsupported not_matched_by_source_action '%s'. "
                    "Only 'delete' is currently applied.",
                    config.not_matched_by_source_action,
                )

        if config.merge_with_schema_evolution:
            builder = builder.withSchemaEvolution()

        builder.execute()
