import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.window import Window

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
    def _resolve_selected_columns(
        cls,
        *,
        candidate_cols: list[str],
        include_cols: list[str] | None,
        exclude_cols: list[str] | None,
        required_cols: list[str] | None = None,
    ) -> list[str]:
        """Resolve selected columns from include/exclude selectors.

        Selection is case-insensitive and preserves ordering from ``candidate_cols``.
        """
        required_cols = required_cols or []
        candidate_lookup = {col.lower(): col for col in candidate_cols}
        selected = list(candidate_cols)

        if include_cols is not None:
            include = {col.lower() for col in include_cols}
            selected = [col for col in candidate_cols if col.lower() in include]

        if exclude_cols is not None:
            exclude = {col.lower() for col in exclude_cols}
            selected = [col for col in selected if col.lower() not in exclude]

        selected_lower = {col.lower() for col in selected}
        for required_col in required_cols:
            required_lower = required_col.lower()
            if required_lower not in candidate_lookup:
                continue
            if required_lower not in selected_lower:
                selected.append(candidate_lookup[required_lower])
                selected_lower.add(required_lower)

        return selected

    @classmethod
    def _dedupe_source_for_merge(
        cls,
        *,
        source_df: DataFrame,
        key_cols: list[str],
        sequence_columns: list[str],
    ) -> DataFrame:
        """Keep one source row per key, preferring newest rows when sequence columns exist."""
        if sequence_columns:
            order_exprs = [f.col(col).desc_nulls_last() for col in sequence_columns]
            window = Window.partitionBy(*[f.col(col) for col in key_cols]).orderBy(*order_exprs)
            return (
                source_df.withColumn("__kelp_rn", f.row_number().over(window))
                .filter(f.col("__kelp_rn") == 1)
                .drop("__kelp_rn")
            )

        return source_df.dropDuplicates(key_cols)

    @classmethod
    def _build_sequence_newer_condition(
        cls,
        *,
        sequence_columns: list[str],
        source_cols: dict[str, str],
        target_cols: dict[str, str],
        source_alias: str,
        target_alias: str,
    ) -> str | None:
        """Build a SQL condition requiring incoming rows to be newer than target rows."""
        if not sequence_columns:
            return None

        comparable = [
            col
            for col in sequence_columns
            if col.lower() in source_cols and col.lower() in target_cols
        ]
        if not comparable:
            return None

        if len(comparable) == 1:
            col = comparable[0]
            src = source_cols[col.lower()]
            tgt = target_cols[col.lower()]
            return f"{source_alias}.`{src}` > {target_alias}.`{tgt}`"

        source_struct = ", ".join(
            [f"{source_alias}.`{source_cols[col.lower()]}`" for col in comparable]
        )
        target_struct = ", ".join(
            [f"{target_alias}.`{target_cols[col.lower()]}`" for col in comparable]
        )
        return f"struct({source_struct}) > struct({target_struct})"

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
        ensure_table_created(spark, kelp_model, target_name)

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

        if not config.unique_keys:
            raise ValueError("'unique_keys' must be set when write_mode='merge'.")

        source_alias = config.source_alias
        target_alias = config.target_alias
        sequence_columns = config.sequence_by or []
        cdc_delete_condition = config.apply_as_delete

        source_df = dataframe

        target_dt = DeltaTable.forName(spark, target_name)
        target_df = target_dt.toDF()
        source_cols = {col.lower(): col for col in source_df.columns}
        target_cols = {col.lower(): col for col in target_df.columns}
        overlapping_cols = [source_cols[col] for col in source_cols if col in target_cols]

        missing_sequence_cols = [col for col in sequence_columns if col.lower() not in source_cols]
        if missing_sequence_cols:
            raise ValueError(
                "Configured sequence_by column(s) missing from source DataFrame for merge: "
                + ", ".join(missing_sequence_cols)
            )

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

        if sequence_columns:
            source_df = cls._dedupe_source_for_merge(
                source_df=source_df,
                key_cols=key_cols,
                sequence_columns=[source_cols[col.lower()] for col in sequence_columns],
            )

        on_parts = [
            f"{source_alias}.`{source_cols[k.lower()]}` = {target_alias}.`{target_cols[k.lower()]}`"
            for k in config.unique_keys
            if k.lower() in source_cols and k.lower() in target_cols
        ]
        merge_condition = " AND ".join(on_parts)

        if config.predicates:
            merge_condition = f"({merge_condition}) AND ({config.predicates})"

        sequence_guard = cls._build_sequence_newer_condition(
            sequence_columns=sequence_columns,
            source_cols=source_cols,
            target_cols=target_cols,
            source_alias=source_alias,
            target_alias=target_alias,
        )

        delete_df = None
        upsert_df = source_df
        if cdc_delete_condition:
            delete_df = source_df.filter(f.expr(cdc_delete_condition))
            upsert_df = source_df.filter(~f.expr(cdc_delete_condition))

        if delete_df is not None:
            delete_merge_condition = merge_condition
            if sequence_guard:
                delete_merge_condition = f"({delete_merge_condition}) AND ({sequence_guard})"

            target_dt.alias(target_alias).merge(
                delete_df.alias(source_alias), delete_merge_condition
            ).whenMatchedDelete().execute()

        builder = target_dt.alias(target_alias).merge(
            upsert_df.alias(source_alias), merge_condition
        )

        selected_target_write_cols = cls._resolve_selected_columns(
            candidate_cols=overlapping_cols,
            include_cols=config.include_at_target_cols,
            exclude_cols=config.exclude_at_target_cols,
            required_cols=key_cols,
        )

        update_candidate_cols = [
            col
            for col in selected_target_write_cols
            if col.lower() not in {k.lower() for k in key_cols}
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

        if sequence_guard:
            if matched_condition:
                matched_condition = f"({matched_condition}) AND ({sequence_guard})"
            else:
                matched_condition = sequence_guard

        update_map: dict[str, str | Column] = {
            f"`{col}`": f"{source_alias}.`{col}`" for col in update_candidate_cols
        }
        if config.ignore_null_updates:
            update_map = {
                f"`{col}`": f"coalesce({source_alias}.`{col}`, {target_alias}.`{col}`)"
                for col in update_candidate_cols
            }

        if update_map:
            if matched_condition:
                builder = builder.whenMatchedUpdate(condition=matched_condition, set=update_map)
            else:
                builder = builder.whenMatchedUpdate(set=update_map)

        insert_map: dict[str, str | Column] = {
            f"`{col}`": f"{source_alias}.`{col}`" for col in selected_target_write_cols
        }

        if config.not_matched_condition:
            builder = builder.whenNotMatchedInsert(
                condition=config.not_matched_condition,
                values=insert_map,
            )
        elif insert_map:
            builder = builder.whenNotMatchedInsert(values=insert_map)

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
