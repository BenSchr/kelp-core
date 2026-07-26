# 0.0.13 (TBD)

## ✨ Features

- feat: improve-sync-metadata - Improved metadata sync code, added multi-threading to column-tag fetching in sdk, added optional spark metadata fetcher through describe and information-schema queries.
- feat: improve-materialization - Reworked materialization around a `mode`-based config (`append`, `overwrite`, `merge`, `scd2`) that rejects unknown fields, adding SCD2 history tracking, a pure merge-planning layer that is unit-testable without Spark, `full_refresh_strategy: replace` to reset a table without dropping it (keeping Unity Catalog grants, tags and history), project-wide `materialization_options`, strict target-name resolution, and an opt-in parallel runner (`run_all(parallel=True)`).

## 🐛 Fixes

- fix: DQX monitoring and quarantine writes set `mergeSchema`, so an additive schema change no longer fails pipelines that write to a shared table from an older kelp or DQX version.
- fix: DQX monitoring rows are grouped by rule fingerprint **and** severity — a rule firing as both error and warning in one batch no longer collapses into a single row with an arbitrarily chosen severity.
- fix: models run by `Runner` receive the SparkSession from the caller. PySpark tracks the active session per thread, so models running in worker threads could not find it.
- fix: `Runner.plan_one` no longer ignores `depends_on`; it returns the dependency-ordered models needed for its target.
- fix: the model registry keys on the full model name instead of the last dotted segment, which silently overwrote same-named models in different schemas.

## ⚠️ Breaking Changes

**Metric Views**
- Metric Views don't allow old syntax `table` and `description` anymore, use the official current yaml reference `source` and `comment`. It is also recommended to move `dimensions` to `fields` syntax this may get deprecated in future.

**Materialization**
- The `materialization:` block is a discriminated union on `mode` and rejects unknown fields, so a stale or misspelled option now fails at parse time instead of being ignored. Renames: `write_mode` → `mode`, `unique_keys` → `keys`, `predicates` → `where`, `apply_as_delete` → `when_deleted`, `merge_with_schema_evolution` → `schema_evolution`, `prevent_full_refresh: true` → `allow_full_refresh: false`, `matched_condition` → `sql_conditions.when_matched`, `not_matched_condition` → `sql_conditions.when_not_matched`, `not_matched_by_source_condition` → `sql_conditions.when_not_matched_by_source`, `not_matched_by_source_action: delete` → `missing_in_source: delete`. The `*_cols` lists are replaced by the `columns`, `track_changes`, `ignore_null_updates_columns` and `insert_only_columns` selectors. `source_alias` / `target_alias` are removed — the merge aliases are always `source` and `target`.
- `materialize()` and `@materialized()` no longer take the `apply_vacuum`, `vacuum_lite`, `apply_optimize`, `apply_quality_checks` and `sync_metadata` keywords. Pass `options={"apply_vacuum": False}` (or a `MaterializationOptions`) instead; project-wide defaults come from `kelp_project.materialization_options`.
- An unqualified `name` passed to `materialize()` / `@materialized()` must resolve to a kelp model and raises `LookupError` otherwise, so a typo can no longer create an unintended table in the session's default catalog and schema. Use a qualified `catalog.schema.table` name to materialize without metadata.
- `materialize()` returns the DataFrame that was written (input minus rows dropped by quality checks) instead of the input DataFrame.



# 0.0.12 (2026-06-15)

## ✨ Features

- feat: auto-ttl ([#21](https://github.com/BenSchr/kelp-core/pull/21)) - Add support to configure automatic time-to-live (TTL) on tables and streaming tables.
- feat: improve skills ([#22](https://github.com/BenSchr/kelp-core/pull/22)) - Enhancing the agent skills of this repo, added claude-plugin and library-skills

## 🐛 Fixes

- fix: merge schema evolution ([#20](https://github.com/BenSchr/kelp-core/pull/20))

# 0.0.11 (2026-06-07)

## ✨ Features

- feat: spark-declarative-dataframes ([#15](https://github.com/BenSchr/kelp-core/pull/15)) - Add support for Spark declarative DataFrames in Kelp materialization. This allows users to define materialized tables using a more concise and expressive syntax.
- feat: external streaming table comments ([#17](https://github.com/BenSchr/kelp-core/pull/17))  - You can now add descriptions to streaming tables outside of sdp.

## ⚠️ Breaking Changes

- Renamed `quarantine_config` to `quality_config` in `ProjectConfig` ([#15](https://github.com/BenSchr/kelp-core/pull/15))

# 0.0.10 (2026-05-26)

## ✨ Features

- feat: integrate-odcs ([#14](https://github.com/BenSchr/kelp-core/pull/14))

## 🐛 Fixes

- fix: view column tags handled correctly (commit [123de42](https://github.com/BenSchr/kelp-core/commit/123de425df80bb111647cd6fa330f280efe4a162))
