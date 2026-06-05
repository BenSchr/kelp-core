import logging
from typing import Any

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


class AdvancedMergeMaterializer:
    """Advanced Delta merge materializer with CDC and SCD2 support.

    This class is intentionally standalone and not wired into the materialization
    factory yet.
    """

    @classmethod
    def run(
        cls,
        *,
        spark: SparkSession,
        dataframe: DataFrame,
        target_name: str,
        unique_keys: list[str] | None = None,
        kelp_model: KelpModel | None = None,
        config: ModelMaterializationConfig | None = None,
        sequence_columns: list[str] | None = None,
        predicates: str | None = None,
        column_list: list[str] | None = None,
        except_column_list: list[str] | None = None,
        scd2_mode: bool = False,
        apply_as_delete: str | None = None,
        apply_as_truncate: str | None = None,
    ) -> None:
        """Apply advanced merge behavior (late-arriving + CDC + SCD2).

        Args:
            spark: Active Spark session.
            dataframe: Source event DataFrame.
            target_name: Delta table to merge into.
            unique_keys: Business key columns (overrides ``config.unique_keys``).
            kelp_model: Optional Kelp model for DDL creation.
            config: Optional merge config; reuses the same fields as normal merge.
            sequence_columns: Optional sequence columns (multi-column supported).
            predicates: Optional target-side predicate to constrain merge matching
                (overrides ``config.predicates``).
            column_list: Optional explicit list of columns to write to the target.
            except_column_list: Optional list of columns to exclude from target writes.
            scd2_mode: Whether to apply SCD2 history behavior.
            apply_as_delete: Optional SQL predicate identifying delete CDC rows.
            apply_as_truncate: Optional SQL predicate identifying truncate CDC rows.
        """
        # ======================================================================
        # FUTURE CONFIG PLACEHOLDERS (dummy variables for future config mapping)
        # ======================================================================
        cdc_delete_condition: str | None = apply_as_delete
        cdc_truncate_condition: str | None = apply_as_truncate

        scd2_start_column = "__START_AT"
        scd2_end_column = "__END_AT"
        scd2_track_history_include_cols: list[str] | None = None
        scd2_track_history_exclude_cols: list[str] | None = None

        ignore_null_updates = False
        merge_with_schema_evolution = True

        # Internal helper column used for multi-column sequencing in SCD2.
        effective_sequence_column = "__kelp_effective_sequence"
        # ======================================================================

        effective_config = config or ModelMaterializationConfig(write_mode="merge")

        resolved_unique_keys = unique_keys or effective_config.unique_keys
        resolved_predicates = predicates if predicates is not None else effective_config.predicates

        if column_list and except_column_list:
            raise ValueError("Specify only one of column_list or except_column_list, not both.")

        if not resolved_unique_keys:
            raise ValueError(
                "'unique_keys' is required for AdvancedMergeMaterializer (or provide config.unique_keys)."
            )

        if not unique_keys:
            unique_keys = resolved_unique_keys

        cls._validate_batch(
            source_df=dataframe,
            unique_keys=resolved_unique_keys,
            sequence_columns=sequence_columns or [],
        )

        existed_before = table_exists(spark, target_name)
        create_table_ddl = kelp_model.get_ddl(if_not_exists=True) if kelp_model else None
        ensure_table_created(
            spark,
            target_name,
            create_table_ddl=create_table_ddl,
            model_name=kelp_model.name if kelp_model else target_name,
        )
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
                config=ModelMaterializationConfig(write_mode="append", options={}),
                create_table_ddl=create_table_ddl,
                model_name=kelp_model.name if kelp_model else target_name,
            )
            return

        source_df = dataframe

        source_df = cls._with_effective_sequence_column(
            source_df=source_df,
            sequence_columns=sequence_columns or [],
            effective_sequence_column=effective_sequence_column,
        )

        target_dt = DeltaTable.forName(spark, target_name)

        if cdc_truncate_condition:
            source_df = cls._apply_sequence_aware_truncate(
                spark=spark,
                source_df=source_df,
                target_name=target_name,
                sequence_columns=sequence_columns or [],
                effective_sequence_column=effective_sequence_column,
                truncate_condition=cdc_truncate_condition,
            )

        if scd2_mode:
            cls._run_scd2(
                target_dt=target_dt,
                source_df=source_df,
                unique_keys=resolved_unique_keys,
                sequence_columns=sequence_columns or [],
                cdc_delete_condition=cdc_delete_condition,
                predicates=resolved_predicates,
                effective_sequence_column=effective_sequence_column,
                scd2_start_column=scd2_start_column,
                scd2_end_column=scd2_end_column,
                history_include_cols=scd2_track_history_include_cols,
                history_exclude_cols=scd2_track_history_exclude_cols,
                config=effective_config,
                column_list=column_list,
                except_column_list=except_column_list,
            )
            return

        cls._run_cdc_type1(
            target_dt=target_dt,
            source_df=source_df,
            unique_keys=resolved_unique_keys,
            sequence_columns=sequence_columns or [],
            cdc_delete_condition=cdc_delete_condition,
            predicates=resolved_predicates,
            ignore_null_updates=ignore_null_updates,
            merge_with_schema_evolution=merge_with_schema_evolution,
            created_now=created_now,
            config=effective_config,
            column_list=column_list,
            except_column_list=except_column_list,
        )

    @classmethod
    def _resolve_selected_columns(
        cls,
        *,
        candidate_cols: list[str],
        column_list: list[str] | None,
        except_column_list: list[str] | None,
        required_cols: list[str] | None = None,
    ) -> list[str]:
        """Resolve selected columns from include/exclude selectors.

        Selection is case-insensitive and preserves the original ordering from
        ``candidate_cols``.
        """
        required_cols = required_cols or []
        candidate_lookup = {c.lower(): c for c in candidate_cols}
        selected = list(candidate_cols)

        if column_list is not None:
            requested = {c.lower() for c in column_list}
            selected = [c for c in candidate_cols if c.lower() in requested]

        if except_column_list is not None:
            excluded = {c.lower() for c in except_column_list}
            selected = [c for c in selected if c.lower() not in excluded]

        selected_lower = {c.lower() for c in selected}
        for req in required_cols:
            req_lower = req.lower()
            if req_lower not in candidate_lookup:
                continue
            if req_lower not in selected_lower:
                selected.append(candidate_lookup[req_lower])
                selected_lower.add(req_lower)

        return selected

    @classmethod
    def _apply_sequence_aware_truncate(
        cls,
        *,
        spark: SparkSession,
        source_df: DataFrame,
        target_name: str,
        sequence_columns: list[str],
        effective_sequence_column: str,
        truncate_condition: str,
    ) -> DataFrame:
        """Apply truncate semantics respecting sequence ordering.

        If truncate events are present, only events strictly after the latest
        truncate sequence remain valid. Events at or before that truncate are
        invalidated.
        """
        truncate_rows = source_df.filter(f.expr(truncate_condition))
        if truncate_rows.isEmpty():
            return source_df

        if not sequence_columns:
            raise ValueError(
                "Sequence-aware truncate requires sequence_columns to determine ordering."
            )

        order_exprs = [f.col(c).desc_nulls_last() for c in sequence_columns]
        latest_truncate = (
            truncate_rows.select(f.col(effective_sequence_column).alias("__truncate_seq"))
            .orderBy(*order_exprs)
            .limit(1)
        )

        spark.sql(f"TRUNCATE TABLE {target_name}")

        non_truncate_rows = source_df.filter(~f.expr(truncate_condition))
        return (
            non_truncate_rows.crossJoin(latest_truncate)
            .filter(f.col(effective_sequence_column) > f.col("__truncate_seq"))
            .drop("__truncate_seq")
        )

    @classmethod
    def _with_effective_sequence_column(
        cls,
        *,
        source_df: DataFrame,
        sequence_columns: list[str],
        effective_sequence_column: str,
    ) -> DataFrame:
        """Materialize a single effective sequence column.

        - Single sequence column: copied as-is.
        - Multi-column sequence: represented as an ordered struct.
        """
        if not sequence_columns:
            return source_df

        if len(sequence_columns) == 1:
            return source_df.withColumn(effective_sequence_column, f.col(sequence_columns[0]))

        return source_df.withColumn(
            effective_sequence_column,
            f.struct(*[f.col(c) for c in sequence_columns]),
        )

    @classmethod
    def _validate_batch(
        cls,
        *,
        source_df: DataFrame,
        unique_keys: list[str],
        sequence_columns: list[str],
    ) -> None:
        """Validate required key and sequence constraints on input batch."""
        missing_keys = [k for k in unique_keys if k not in source_df.columns]
        if missing_keys:
            raise ValueError(
                "Key column(s) not found in source DataFrame: " + ", ".join(missing_keys)
            )

        missing_sequence_cols = [c for c in sequence_columns if c not in source_df.columns]
        if missing_sequence_cols:
            raise ValueError(
                "Sequence column(s) not found in source DataFrame: "
                + ", ".join(missing_sequence_cols)
            )

        if not sequence_columns:
            return

        null_aggregates = [
            f.sum(f.when(f.col(col_name).isNull(), f.lit(1)).otherwise(f.lit(0))).alias(col_name)
            for col_name in [*unique_keys, *sequence_columns]
        ]
        counts = source_df.agg(*null_aggregates).first()
        if counts is None:
            return

        null_columns = [
            col_name
            for col_name in [*unique_keys, *sequence_columns]
            if counts[col_name] and counts[col_name] > 0
        ]
        if null_columns:
            raise ValueError(
                "Validation failed: null values found in required key/sequence "
                "columns: " + ", ".join(null_columns)
            )

    @classmethod
    def _run_cdc_type1(
        cls,
        *,
        target_dt: DeltaTable,
        source_df: DataFrame,
        unique_keys: list[str],
        sequence_columns: list[str],
        cdc_delete_condition: str | None,
        predicates: str | None,
        ignore_null_updates: bool,
        merge_with_schema_evolution: bool,
        created_now: bool,
        config: ModelMaterializationConfig,
        column_list: list[str] | None,
        except_column_list: list[str] | None,
    ) -> None:
        """Apply classic upsert/delete merge semantics (Type 1 style)."""
        target_df = target_dt.toDF()
        target_cols = {c.lower(): c for c in target_df.columns}
        source_cols = {c.lower(): c for c in source_df.columns}
        source_alias = config.source_alias
        target_alias = config.target_alias

        key_cols = [k for k in unique_keys if k.lower() in source_cols and k.lower() in target_cols]
        if not key_cols:
            raise ValueError("No valid merge keys found in both source and target.")

        should_dedupe_source = bool(sequence_columns)
        if should_dedupe_source:
            source_df = cls._dedupe_source_for_type1(
                source_df=source_df,
                key_cols=key_cols,
                sequence_columns=sequence_columns,
            )

        delete_df = None
        upsert_df = source_df
        if cdc_delete_condition:
            delete_df = source_df.filter(f.expr(cdc_delete_condition))
            upsert_df = source_df.filter(~f.expr(cdc_delete_condition))

        merge_condition = " AND ".join(
            [
                f"{source_alias}.`{source_cols[k.lower()]}` = {target_alias}.`{target_cols[k.lower()]}`"
                for k in key_cols
            ]
        )
        if predicates:
            merge_condition = f"({merge_condition}) AND ({predicates})"

        sequence_guard = cls._build_sequence_newer_condition(
            sequence_columns=sequence_columns,
            source_cols=source_cols,
            target_cols=target_cols,
            source_alias=source_alias,
            target_alias=target_alias,
        )

        delete_merge_condition = merge_condition
        if sequence_guard:
            delete_merge_condition = f"({merge_condition}) AND ({sequence_guard})"

        if delete_df is not None:
            target_dt.alias(target_alias).merge(
                delete_df.alias(source_alias), delete_merge_condition
            ).whenMatchedDelete().execute()

        overlapping = [source_cols[c] for c in source_cols if c in target_cols]
        selected_target_write_cols = cls._resolve_selected_columns(
            candidate_cols=overlapping,
            column_list=column_list,
            except_column_list=except_column_list,
            required_cols=key_cols,
        )

        update_cols = [c for c in overlapping if c.lower() not in {k.lower() for k in key_cols}]
        update_cols = [
            c for c in update_cols if c.lower() in {x.lower() for x in selected_target_write_cols}
        ]

        if config.matched_update_include_cols:
            include = {c.lower() for c in config.matched_update_include_cols}
            update_cols = [c for c in update_cols if c.lower() in include]
        if config.matched_update_exclude_cols:
            exclude = {c.lower() for c in config.matched_update_exclude_cols}
            update_cols = [c for c in update_cols if c.lower() not in exclude]

        update_map: dict[str, str | Column] = {
            f"`{c}`": f"{source_alias}.`{c}`" for c in update_cols
        }
        if ignore_null_updates:
            update_map = {
                f"`{c}`": f"coalesce({source_alias}.`{c}`, {target_alias}.`{c}`)"
                for c in update_cols
            }

        insert_cols = list(selected_target_write_cols)
        insert_map: dict[str, str | Column] = {
            f"`{c}`": f"{source_alias}.`{c}`" for c in insert_cols
        }

        builder = target_dt.alias(target_alias).merge(
            upsert_df.alias(source_alias), merge_condition
        )
        if update_map and not created_now:
            matched_condition = config.matched_condition
            if matched_condition is None:
                condition_cols = list(update_cols)
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

            if matched_condition:
                builder = builder.whenMatchedUpdate(condition=matched_condition, set=update_map)
            else:
                builder = builder.whenMatchedUpdate(set=update_map)

        if config.not_matched_condition:
            builder = builder.whenNotMatchedInsert(
                condition=config.not_matched_condition, values=insert_map
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

        if merge_with_schema_evolution and config.merge_with_schema_evolution:
            builder = builder.withSchemaEvolution()
        builder.execute()

    @classmethod
    def _dedupe_source_for_type1(
        cls,
        *,
        source_df: DataFrame,
        key_cols: list[str],
        sequence_columns: list[str],
    ) -> DataFrame:
        """Keep one source row per key for Type1 merge.

        If sequence columns are provided, the latest row per key wins.
        Otherwise, duplicate keys are collapsed arbitrarily to avoid
        DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
        """
        if sequence_columns:
            order_exprs = [f.col(c).desc_nulls_last() for c in sequence_columns]
            window = Window.partitionBy(*[f.col(c) for c in key_cols]).orderBy(*order_exprs)
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
        """Build a condition requiring incoming events to be newer than target rows."""
        if not sequence_columns:
            return None

        comparable = [
            col_name
            for col_name in sequence_columns
            if col_name.lower() in source_cols and col_name.lower() in target_cols
        ]
        if not comparable:
            return None

        if len(comparable) == 1:
            col_name = comparable[0]
            src = source_cols[col_name.lower()]
            tgt = target_cols[col_name.lower()]
            return f"{source_alias}.`{src}` > {target_alias}.`{tgt}`"

        source_struct = ", ".join(
            [f"{source_alias}.`{source_cols[c.lower()]}`" for c in comparable]
        )
        target_struct = ", ".join(
            [f"{target_alias}.`{target_cols[c.lower()]}`" for c in comparable]
        )
        return f"struct({source_struct}) > struct({target_struct})"

    @classmethod
    def _run_scd2(
        cls,
        *,
        target_dt: Any,
        source_df: DataFrame,
        unique_keys: list[str],
        sequence_columns: list[str],
        cdc_delete_condition: str | None,
        predicates: str | None,
        effective_sequence_column: str,
        scd2_start_column: str,
        scd2_end_column: str,
        history_include_cols: list[str] | None,
        history_exclude_cols: list[str] | None,
        config: ModelMaterializationConfig,
        column_list: list[str] | None,
        except_column_list: list[str] | None,
    ) -> None:
        """Apply SCD2 semantics with active-row expiration and version insertion."""
        if not sequence_columns:
            raise ValueError("SCD2 mode requires at least one sequence column.")

        if effective_sequence_column not in source_df.columns:
            raise ValueError(
                f"Effective sequence column '{effective_sequence_column}' is missing from source."
            )
        target_df = target_dt.toDF()
        target_cols = {c.lower(): c for c in target_df.columns}
        source_cols = {c.lower(): c for c in source_df.columns}
        source_alias = config.source_alias
        target_alias = config.target_alias

        if (
            scd2_start_column.lower() not in target_cols
            or scd2_end_column.lower() not in target_cols
        ):
            raise ValueError("SCD2 target table must include '__START_AT' and '__END_AT' columns.")

        key_cols = [k for k in unique_keys if k.lower() in source_cols and k.lower() in target_cols]
        if not key_cols:
            raise ValueError("No valid SCD2 keys found in both source and target.")

        active_pred = f"{target_alias}.`{target_cols[scd2_end_column.lower()]}` IS NULL"
        key_join = " AND ".join(
            [
                f"{source_alias}.`{source_cols[k.lower()]}` = {target_alias}.`{target_cols[k.lower()]}`"
                for k in key_cols
            ]
        )
        if predicates:
            key_join = f"({key_join}) AND ({predicates})"
        active_join = f"({key_join}) AND ({active_pred})"

        source_business_cols = [
            source_cols[c]
            for c in source_cols
            if c in target_cols
            and c not in {scd2_start_column.lower(), scd2_end_column.lower()}
            and c != effective_sequence_column.lower()
        ]

        source_business_cols = cls._resolve_selected_columns(
            candidate_cols=source_business_cols,
            column_list=column_list,
            except_column_list=except_column_list,
            required_cols=key_cols,
        )

        tracked_cols = [
            c for c in source_business_cols if c.lower() not in {k.lower() for k in key_cols}
        ]

        if history_include_cols:
            include = {c.lower() for c in history_include_cols}
            tracked_cols = [c for c in tracked_cols if c.lower() in include]
        if history_exclude_cols:
            exclude = {c.lower() for c in history_exclude_cols}
            tracked_cols = [c for c in tracked_cols if c.lower() not in exclude]

        change_condition = build_null_safe_change_condition(
            source_alias=source_alias,
            target_alias=target_alias,
            columns=tracked_cols,
        )

        if config.matched_condition:
            change_condition = config.matched_condition

        sequence_steps = cls._sorted_sequence_steps(
            source_df=source_df,
            sequence_columns=sequence_columns,
        )

        for sequence_step in sequence_steps:
            step_df = cls._filter_sequence_step(
                source_df=source_df,
                sequence_columns=sequence_columns,
                sequence_step=sequence_step,
            ).dropDuplicates(key_cols)

            delete_df = None
            upsert_df = step_df
            if cdc_delete_condition:
                delete_df = step_df.filter(f.expr(cdc_delete_condition))
                upsert_df = step_df.filter(~f.expr(cdc_delete_condition))

            if delete_df is not None and not delete_df.isEmpty():
                target_dt.alias(target_alias).merge(
                    delete_df.alias(source_alias), active_join
                ).whenMatchedUpdate(
                    set={f"`{scd2_end_column}`": f"{source_alias}.`{effective_sequence_column}`"}
                ).execute()

            if upsert_df.isEmpty():
                continue

            late_upsert_df, regular_upsert_df = cls._split_late_arriving_scd2_rows(
                target_dt=target_dt,
                upsert_df=upsert_df,
                key_cols=key_cols,
                source_cols=source_cols,
                target_cols=target_cols,
                effective_sequence_column=effective_sequence_column,
                scd2_start_column=scd2_start_column,
                scd2_end_column=scd2_end_column,
            )

            if change_condition and not regular_upsert_df.isEmpty():
                target_dt.alias(target_alias).merge(
                    regular_upsert_df.alias(source_alias), active_join
                ).whenMatchedUpdate(
                    condition=change_condition,
                    set={f"`{scd2_end_column}`": f"{source_alias}.`{effective_sequence_column}`"},
                ).execute()

            insert_map: dict[str, str | Column] = {
                f"`{target_cols[c.lower()]}`": f"{source_alias}.`{source_cols[c.lower()]}`"
                for c in source_business_cols
                if c.lower() in target_cols and c.lower() in source_cols
            }
            insert_map[f"`{scd2_start_column}`"] = f"{source_alias}.`{effective_sequence_column}`"
            insert_map[f"`{scd2_end_column}`"] = "NULL"

            if not regular_upsert_df.isEmpty():
                insert_builder = target_dt.alias(target_alias).merge(
                    regular_upsert_df.alias(source_alias), active_join
                )
                if config.not_matched_condition:
                    insert_builder = insert_builder.whenNotMatchedInsert(
                        condition=config.not_matched_condition,
                        values=insert_map,
                    )
                else:
                    insert_builder = insert_builder.whenNotMatchedInsert(values=insert_map)

                if config.merge_with_schema_evolution:
                    insert_builder = insert_builder.withSchemaEvolution()

                insert_builder.execute()

            if not late_upsert_df.isEmpty():
                late_covering_join = (
                    f"({key_join}) "
                    f"AND ({target_alias}.`{target_cols[scd2_start_column.lower()]}` < "
                    f"{source_alias}.`{effective_sequence_column}`) "
                    f"AND ({target_alias}.`{target_cols[scd2_end_column.lower()]}` IS NULL "
                    f"OR {target_alias}.`{target_cols[scd2_end_column.lower()]}` > "
                    f"{source_alias}.`{effective_sequence_column}`)"
                )
                target_dt.alias(target_alias).merge(
                    late_upsert_df.alias(source_alias), late_covering_join
                ).whenMatchedUpdate(
                    set={f"`{scd2_end_column}`": f"{source_alias}.`{effective_sequence_column}`"}
                ).execute()

                late_insert_map = dict(insert_map)
                late_insert_map[f"`{scd2_end_column}`"] = (
                    f"coalesce({source_alias}.`__kelp_next_start`, "
                    f"{source_alias}.`__kelp_active_start`)"
                )
                target_dt.alias(target_alias).merge(
                    late_upsert_df.alias(source_alias), "1 = 0"
                ).whenNotMatchedInsert(values=late_insert_map).execute()

    @classmethod
    def _sorted_sequence_steps(
        cls,
        *,
        source_df: DataFrame,
        sequence_columns: list[str],
    ) -> list[tuple[Any, ...]]:
        """Collect and sort distinct sequence values ascending."""
        steps = source_df.select(*[f.col(c) for c in sequence_columns]).dropDuplicates().collect()
        return sorted([tuple(step[c] for c in sequence_columns) for step in steps])

    @classmethod
    def _filter_sequence_step(
        cls,
        *,
        source_df: DataFrame,
        sequence_columns: list[str],
        sequence_step: tuple[Any, ...],
    ) -> DataFrame:
        """Filter source rows for one exact sequence step."""
        condition: Column | None = None
        for index, column_name in enumerate(sequence_columns):
            col_condition = f.col(column_name) == f.lit(sequence_step[index])
            condition = col_condition if condition is None else (condition & col_condition)
        if condition is None:
            return source_df
        return source_df.filter(condition)

    @classmethod
    def _split_late_arriving_scd2_rows(
        cls,
        *,
        target_dt: Any,
        upsert_df: DataFrame,
        key_cols: list[str],
        source_cols: dict[str, str],
        target_cols: dict[str, str],
        effective_sequence_column: str,
        scd2_start_column: str,
        scd2_end_column: str,
    ) -> tuple[DataFrame, DataFrame]:
        """Split SCD2 upserts into late-arriving and regular rows.

        A late-arriving row has sequence < active row start for the same key.
        """
        active_start_col = target_cols[scd2_start_column.lower()]
        active_end_col = target_cols[scd2_end_column.lower()]

        source_with_id = upsert_df.withColumn("__kelp_src_row_id", f.monotonically_increasing_id())

        target_snapshot = target_dt.toDF()

        active_df = target_snapshot.filter(f.col(active_end_col).isNull()).select(
            *[
                f.col(target_cols[k.lower()]).alias(f"__kelp_active_key_{k.lower()}")
                for k in key_cols
            ],
            f.col(active_start_col).alias("__kelp_active_start"),
        )

        if active_df.isEmpty():
            return upsert_df.limit(0), upsert_df

        active_join_condition: Column | None = None
        for key_col in key_cols:
            source_key = source_cols[key_col.lower()]
            active_key_alias = f"__kelp_active_key_{key_col.lower()}"
            key_condition = f.col(f"s.{source_key}") == f.col(f"a.{active_key_alias}")
            active_join_condition = (
                key_condition
                if active_join_condition is None
                else (active_join_condition & key_condition)
            )

        if active_join_condition is None:
            return upsert_df.limit(0), upsert_df

        joined_active = source_with_id.alias("s").join(
            f.broadcast(active_df).alias("a"), active_join_condition, "left"
        )

        late_condition = f.col("a.__kelp_active_start").isNotNull() & (
            f.col(f"s.{effective_sequence_column}") < f.col("a.__kelp_active_start")
        )

        source_projection_s = [f.col(f"s.{c}").alias(c) for c in upsert_df.columns]
        late_candidates = joined_active.filter(late_condition).select(
            *source_projection_s,
            f.col("s.__kelp_src_row_id").alias("__kelp_src_row_id"),
            f.col("a.__kelp_active_start").alias("__kelp_active_start"),
        )

        if late_candidates.isEmpty():
            return upsert_df.limit(0), upsert_df

        regular_df = joined_active.filter(~late_condition).select(*source_projection_s)

        # Reuse cached target snapshot; filter to only keys present in late
        # candidates to reduce the range-join search space.
        late_key_df = late_candidates.select(
            *[
                f.col(source_cols[k.lower()]).alias(f"__kelp_start_key_{k.lower()}")
                for k in key_cols
            ]
        ).dropDuplicates()

        starts_df = target_snapshot.select(
            *[
                f.col(target_cols[k.lower()]).alias(f"__kelp_start_key_{k.lower()}")
                for k in key_cols
            ],
            f.col(active_start_col).alias("__kelp_start_candidate"),
        ).join(
            f.broadcast(late_key_df),
            [f"__kelp_start_key_{k.lower()}" for k in key_cols],
            "inner",
        )

        next_start_join_condition: Column | None = None
        for key_col in key_cols:
            source_key = source_cols[key_col.lower()]
            start_key_alias = f"__kelp_start_key_{key_col.lower()}"
            key_condition = f.col(f"s.{source_key}") == f.col(f"t.{start_key_alias}")
            next_start_join_condition = (
                key_condition
                if next_start_join_condition is None
                else (next_start_join_condition & key_condition)
            )

        if next_start_join_condition is None:
            return upsert_df.limit(0), upsert_df

        next_start_join_condition = next_start_join_condition & (
            f.col("t.__kelp_start_candidate") > f.col(f"s.{effective_sequence_column}")
        )

        next_start_df = (
            late_candidates.alias("s")
            .join(starts_df.alias("t"), next_start_join_condition, "left")
            .groupBy(f.col("s.__kelp_src_row_id"))
            .agg(f.min(f.col("t.__kelp_start_candidate")).alias("__kelp_next_start"))
        )

        late_joined = late_candidates.alias("l").join(
            next_start_df.alias("n"),
            f.col("l.__kelp_src_row_id") == f.col("n.__kelp_src_row_id"),
            "left",
        )

        source_projection_l = [f.col(f"l.{c}").alias(c) for c in upsert_df.columns]
        late_df = late_joined.select(
            *source_projection_l,
            f.col("l.__kelp_active_start").alias("__kelp_active_start"),
            f.col("n.__kelp_next_start").alias("__kelp_next_start"),
        )

        return late_df, regular_df
