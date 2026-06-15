---
name: kelp-migration
description: >
  Skill when migrating existing Spark tables and pipelines to Kelp. Use when migrating existing tables to Kelp metadata, refactoring hardcoded catalog/schema references, extracting metadata from code to YAML, converting manual expectations to kelp quality checks. Espacially when the user instructs a migration of existing project to Kelp.
---

# kelp-migration

Use this skill to migrate existing Spark tables and pipelines to Kelp. This skill is especially useful when the user instructs a migration of existing tables to Kelp metadata. It provides guidance on how to transform.

## Prequerisites

**Always** use the `kelp-base` skill and its references to get a good understanding of how Kelp works and how to use it in your Spark projects.

## Usage

**USE FOR:**
- Migrating existing Spark tables and pipelines to Kelp metadata
- On user instruction to migrate existing tables to Kelp metadata

**USE NOT FOR:**
- Creating or modifying non spark tables or views

## References

| Reference | Description |
| --- | --- |
| [SDP-Migration](references/sdp-migration.md) | Guide on how to migrate existing Spark Declarative Pipelines to use Kelp metadata |
| [Spark-Tables](references/spark-table-migration.md) | Guide on how to migrate existing Spark tables to use Kelp metadata |

## Migration Strategy

```
ASSESS → PLAN → EXTRACT → CONVERT → VALIDATE → ITERATE

├─ ASSESS: Inventory existing code (tables, schemas, quality checks)
├─ PLAN: Prioritize by layer (bronze → silver → gold)
├─ EXTRACT: Generate YAML from code/catalog
├─ CONVERT: Refactor Python to kelp patterns
├─ VALIDATE: Use `kelp validate` to check metadata or other instructed methods
└─ ITERATE: Migrate incrementally, one layer at a time
```

## Troubleshooting

- If you encounter issues with Kelp metadata, ensure that your changes are valid and follow the expected structure. Use the `kelp validate` command to check for any errors or inconsistencies in your metadata files.
- Ask the user how to proceed if you are unsure about the next steps or if you encounter unexpected issues. Preffer to use any provided tool if any provided for user interaction and question asking.
