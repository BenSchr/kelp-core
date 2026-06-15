---
name: kelp-base
description: >
  Base skill when working with Kelp projects using Spark and Spark Declarative Pipelines. Use when doing any spark or metadata related work - Create or modify tables, updating table properties, create or modify other assets like metric views
---

# kelp-base

Base skill when working with Kelp projects using Spark and Spark Declarative Pipelines. Use when doing any spark or metadata related work - Create or modify tables, updating table properties, create or modify other assets like metric views

## Usage

**USE FOR:**
- Any kelp-core related work
- Creating and modifying spark tables and views
- Updating table properties
- Creating and modifying other assets like metric views

**USE NOT FOR:**
- Non spark related work
- Non kelp-core related work
- Creating or modifying non spark tables or views

## References

| Reference | Description |
| --- | --- |
| [Kelp-Project](references/kelp-project.md) | Overview of the Kelp project, its goals, and how it integrates with Spark and metadata management. |
| [SDP-Pipelines](references/sdp-pipelines.md) | Building Spark Declarative Pipelines with Kelp metadata |
| [Spark-Tables](references/spark-tables.md) | Creating and managing Spark tables using Kelp metadata |

## CLI

After editing Kelp metadata, use the following command to validate your changes:

```bash
kelp validate --target <default target>
```


## Troubleshooting

- If you encounter issues with Kelp metadata, ensure that your changes are valid and follow the expected structure. Use the `kelp validate` command to check for any errors or inconsistencies in your metadata files.
- Ask the user how to proceed if you are unsure about the next steps or if you encounter unexpected issues. Preffer to use any provided tool if any provided for user interaction and question asking.
