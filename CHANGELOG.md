# 0.0.12 (TBD)

## ✨ Features

- feat: auto-ttl ([#21](https://github.com/BenSchr/kelp-core/pull/21)) - Add support to configure automatic time-to-live (TTL) on tables and streaming tables.

## 🐛 Fixes

- fix: merge schema evolution ([#20](https://github.com/BenSchr/kelp-core/pull/20))

## ⚠️ Breaking Changes

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
