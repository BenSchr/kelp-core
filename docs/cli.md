---
hide:
  - navigation
---
<style> .md-content .md-typeset h1 { display: none; } </style> 

# CLI Reference

The Kelp CLI provides commands for managing your project configuration, validating metadata, syncing with Unity Catalog, and initializing new projects.

## Installation

Kelp is installed via Python package management. Once installed, you can access the CLI via:

```bash
uv run kelp --help
```

Or if Kelp is installed in your Python environment:

```bash
kelp --help
```

## Core Commands

### `kelp version`

Display the current version of Kelp.

```bash
kelp version
```

Returns the Kelp banner and version information.

### `kelp validate`

Validate your Kelp project configuration and catalog.

```bash
kelp validate \
  --config kelp_project.yml \
  --target prod \
  --debug
```


**Options:**

```
  -c, --config TEXT    Path to kelp_project.yml (optional, will auto-detect if
                        not provided)
  -t, --target TEXT    Target to use for variable resolution
  -m, --manifest TEXT  Path to manifest JSON file (skips source file loading)
  --debug              Enable debug logging
```

**Output:**

Shows validation status and loads configuration information:

```
✓ Configuration is valid!
Project config loaded from: /path/to/kelp_project.yml
Target environment: prod
Runtime variables: {'catalog': 'analytics_prod', 'schema': 'core_prod'}
Relative models path: ./kelp_metadata/models
Models found: 42
Relative functions path: ./kelp_metadata/functions
Functions found: 8
Relative ABACs path: ./kelp_metadata/abacs
ABAC policies found: 5
Relative metrics path: ./kelp_metadata/metrics
Metric views found: 3
```

### `kelp manifest`

Generate a manifest JSON file for validation without loading source files.

```bash
kelp manifest \
  --config kelp_project.yml \
  --target prod \
  --output manifest.json
```

**Options:**

```
  -c, --config TEXT  Path to kelp_project.yml (optional, will auto-detect if
                     not provided)
  -t, --target TEXT  Target to use for variable resolution
  -o, --output TEXT  Output path for the manifest JSON file  [default:
                     manifest.json]
  --debug            Enable debug logging
```

### `kelp json-schema`

Generate a JSON schema for `kelp_project.yml` configuration validation.

```bash
kelp json-schema \
  --output kelp_json_schema.json \
  --vscode
```

**Options:**

```
  -o, --output PATH  Output path for JSON schema (default: current directory)
  --dry-run          Preview output without writing
  --vscode           Create/update VS Code .vscode/settings.json with YAML
                      schema config
```

### `kelp check-policies`

Evaluate metadata governance policies against your local table catalog.

```bash
kelp check-policies \
  --config kelp_project.yml \
  --target prod
```

**Options:**

```
  -c, --config TEXT             Path to kelp_project.yml (optional, will auto-
                                detect if not provided)
  -t, --target TEXT             Target to use for variable resolution
  -m, --manifest TEXT           Path to manifest JSON file (skips source file
                                loading)
  --severity TEXT               Only show violations at this severity level or
                                above: 'warn' or 'error'
  --fail-on TEXT                Exit with code 1 when violations of this
                                severity are found: 'warn' or 'error'
                                [default: error]
  --fast-exit / --no-fast-exit  Stop policy evaluation on first violating
                                policy per model. Defaults to
                                policy_config.fast_exit when not provided.
  --debug                       Enable debug logging
```

**Output with violations:**

```
⚠ [WARN]  catalog.schema.bronze_customers — Table 'catalog.schema.bronze_customers' is missing a description.
✗ [ERROR] catalog.schema.silver_orders.order_id — Column 'order_id' in table '...' is missing a description.

Policy check complete: 1 error(s), 1 warning(s) across 15 models(s).
```

**Output when all checks pass:**

```
✓ Policy check complete: no violations found (15 models(s) checked).
```

**Use in CI/CD:**

```bash
# Fail the pipeline on any policy error
kelp check-policies --target prod --fail-on error
```

### `kelp sync-from-catalog`

Fetch table metadata from Databricks Unity Catalog and generate a YAML model definition.

```bash
kelp sync-from-catalog \
  "analytics_prod.core.customers" \
  --profile my-profile \
  --output customers.yml \
```

**Arguments:**
```
  TABLE_PATH  Fully qualified table name, e.g. database.schema.table
              [required]
```

**Options:**
```
  -p, --profile TEXT    Databricks CLI profile to use
  --include-properties  Include all table properties in the output YAML (use
                        with caution, may include many properties)
  -o, --output TEXT     Path to output file for YAML (optional)
  --dry-run             Preview output without writing
```

**Output:**

Generates a `kelp_models` YAML structure:

```yaml
kelp_models:
  - name: customers
    catalog: analytics_prod
    schema: core
    description: Customer dimension table
    columns:
      - name: customer_id
        data_type: INT
        description: Primary key
      - name: customer_name
        data_type: STRING
      - name: email
        data_type: STRING
```

### `kelp sync-from-pipeline`

Fetch table definitions from a Databricks Spark Declarative Pipeline.

```bash
kelp sync-from-pipeline \
  --id abc123def456 \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output sync_report.log
```

**Options:**
```
  --id TEXT            Databricks pipeline ID (optional, will auto-detect if
                       not provided)
  -c, --config TEXT    Path to kelp_project.yml (optional, will auto-detect if
                       not provided)
  -t, --target TEXT    Target to use for variable resolution
  -m, --manifest TEXT  Path to manifest JSON file (skips source file loading)
  -p, --profile TEXT   Databricks CLI profile to use
  -o, --output TEXT    Path to output file for sync log
  --dry-run            Preview output without writing
  --debug              Enable debug logging
```

**Output:**

Fetches tables from pipeline and creates/updates YAML files. Shows real-time progress:

```
Fetching tables from pipeline abc123def456...
  • bronze_customers
  • bronze_orders
  • silver_customers_cleaned
Fetched 3 tables from pipeline abc123def456

✓ Sync complete: 3/3 tables synced
  - 1 created
  - 2 updated
  - 0 unchanged
```

### `kelp sync-local-catalog`

Sync local YAML files with remote Unity Catalog tables and metric views.

```bash
kelp sync-local-catalog \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output sync_report.log \
  --dry-run
```

Sync a specific object by name:

```bash
kelp sync-local-catalog \
  "customers" \
  --config kelp_project.yml \
  --target prod
```

**Options:**
```
  --name TEXT          Table or metric view name/FQN to sync
  -c, --config TEXT    Path to kelp_project.yml (optional, will auto-detect if
                       not provided)
  -t, --target TEXT    Target to use for variable resolution
  -m, --manifest TEXT  Path to manifest JSON file (skips source file loading)
  -p, --profile TEXT   Databricks CLI profile to use
  -o, --output TEXT    Path to output file for sync log
  --dry-run            Preview output without writing
  --debug              Enable debug logging
```

**Output:**

Shows progress and summary of changes:

```
Syncing tables |████████████████████| 100%
Syncing metric views |████████████████| 100%

Dry-run report:
  Would update:
    - customers -> ./kelp_metadata/models/customers.yml
    - orders -> ./kelp_metadata/models/orders.yml
  Skipped:
    - transactions (not in remote)
  Unchanged: 2
  Tables checked: 5
  Metric views checked: 3
```

## Generate Commands

### `kelp generate-ddl`

Generate DDL (Data Definition Language) CREATE TABLE statements from metadata.

```bash
kelp generate-ddl \
  --config kelp_project.yml \
  --target prod \
  --output create_tables.sql \
  --dry-run
```

**Options:**
```
  -c, --config TEXT    Path to kelp_project.yml (optional, will auto-detect if
                       not provided)
  -t, --target TEXT    Target to use for variable resolution
  -m, --manifest TEXT  Path to manifest JSON file (skips source file loading)
  --dry-run            Preview output without writing
  --debug              Enable debug logging
  -o, --output TEXT    Path to output file for DDL statement (optional,
                       defaults to stdout)
```

**Output:**

Generates CREATE TABLE statements:

```sql
-- bronze/bronze_customers.yml
CREATE TABLE IF NOT EXISTS kelp_catalog.kelp_bronze.bronze_customers (
  user_id STRING NOT NULL COMMENT 'Internal user identifier',
  first_name STRING COMMENT 'Customer given name',
  last_name STRING COMMENT 'Customer family name',
  country STRING COMMENT 'Customer country'
) USING DELTA
TBLPROPERTIES ('domain' = 'customers', 'stage' = 'bronze');
```

### `kelp generate-alter-statements`

Generate ALTER TABLE statements to sync existing tables with metadata changes.

```bash
kelp generate-alter-statements \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output alter_tables.sql \
  --dry-run
```

**Options:**
```
  -c, --config TEXT    Path to kelp_project.yml (optional, will auto-detect if
                       not provided)
  -t, --target TEXT    Target to use for variable resolution
  -m, --manifest TEXT  Path to manifest JSON file (skips source file loading)
  -p, --profile TEXT   Databricks CLI profile to use
  --dry-run            Preview output without writing
  --debug              Enable debug logging
  -o, --output TEXT    Path to output file for ALTER TABLE
  --silent             Only output ALTER TABLE statements, suppressing other
                       logs
```

**Output:**

Generates ALTER statements for metadata updates:

```sql
-- Update descriptions and tags
ALTER TABLE kelp_catalog.kelp_bronze.bronze_customers 
  SET TBLPROPERTIES ('owner' = 'analytics-team');

ALTER TABLE kelp_catalog.kelp_bronze.bronze_customers 
  ALTER COLUMN user_id COMMENT 'Internal user identifier';
```

## Init Commands

### `kelp init`

Initialize a new Kelp project with default structure and configuration.

```bash
kelp init .
```

**Creates:**

- `kelp_project.yml` - Base project configuration
- `kelp_metadata/models/` - Directory for model definitions
- `kelp_metadata/metrics/` - Directory for metric view definitions
- `kelp_metadata/functions/` - Directory for function definitions (optional)
- `.gitkeep` files for git tracking

## Open Data Contract Commands
### `kelp odcs import`
Import Open Data Contract specifications and generate YAML metadata.

```bash
Usage: kelp odcs import [OPTIONS] SOURCE

  Import a data contract into kelp metadata format.

Arguments:
  SOURCE  Path to the data contract YAML file.  [required]

Options:
  -o, --output PATH     Output file path. Prints to stdout if omitted.
  --generate-dqx-rules  Generate DQX quality rules from the contract.
  --patch               Patch existing Kelp model YAML files instead of
                        printing all models.
  --dry-run             Preview output without writing
  -c, --config TEXT     Path to kelp_project.yml (optional, will auto-detect
                        if not provided)
  -t, --target TEXT     Target to use for variable resolution
  -m, --manifest TEXT   Path to manifest JSON file (skips source file loading)
  --debug               Enable debug logging
  --help                Show this message and exit.
```

Example usage:

```bash
kelp odcs import \
  --generate-dqx-rules \
  --patch \
  --config kelp_project.yml \
  --target prod \
  my_data_contract.yml
```

Imports the data contract, generates DQX rules, and patches existing model YAML files based on the contract specifications.

### `kelp odcs export`

Export a Kelp model to Open Data Contract Standard format.

```bash
Usage: kelp odcs export [OPTIONS] MODEL

  Export a kelp model to Open Data Contract Standard format.

Arguments:
  MODEL  Name of the kelp model to export.  [required]

Options:
  -o, --output PATH     Output file path. Prints to stdout if omitted.
  -c, --config TEXT     Path to kelp_project.yml (optional, will auto-detect
                        if not provided)
  -t, --target TEXT     Target to use for variable resolution
  -m, --manifest TEXT   Path to manifest JSON file (skips source file loading)
  --debug               Enable debug logging
  --include-server      Include ODCS server (database/catalog and schema) when
                        model contains catalog and schema.
  --patch               Patch an existing contract YAML file, updating only
                        the matching schema.
  --dry-run             Preview output without writing
  --contract-file PATH  Existing contract YAML file to patch when --patch is
                        used.
  --help                Show this message and exit.
```

Example usage:

```bash
kelp odcs export \
  --include-server \
  --patch \
  --config kelp_project.yml \
  --target prod \
  --contract-file existing_contract.yml \
  customers
```

Exports the `customers` model to ODCS format, including server information, and patches the existing contract YAML file with the new schema information.

## Configuration Environment Variables

Configure Kelp CLI behavior using environment variables:

### `KELP_PROJECT_FILE`

Path to the `kelp_project.yml` file (overrides auto-discovery):

```bash
export KELP_PROJECT_FILE="./custom_config.yml"
kelp validate
```

### `KELP_MANIFEST_FILE`
Path to a manifest JSON file for validation (skips source file loading):
```bash
export KELP_MANIFEST_FILE="./manifest.json"
kelp validate
```

### `KELP_TARGET`

Default environment target:

```bash
export KELP_TARGET="prod"
kelp validate  # Uses "prod" if not overridden by --target
```

### `KELP_PROFILE`

Default Databricks CLI profile:

```bash
export KELP_PROFILE="my-workspace"
kelp sync-from-catalog "analytics.core.customers"
```



## Common Workflows

### Setup New Project

```bash
# Create project structure
kelp init project --path ./my_project --catalog my_catalog

# Generate JSON schema for IDE support
cd my_project
kelp json-schema

# Validate configuration
kelp validate
```

### Import Existing Table Metadata

```bash
# Fetch from Databricks and generate YAML
kelp sync-from-catalog \
  "analytics_prod.core.customers" \
  -p my-profile \
  -o kelp_metadata/models/customers.yml

# Validate the generated file
kelp validate
```

### Validate Before Deployment

```bash
# Validate development environment
kelp validate --target dev

# Validate production environment
kelp validate --target prod
```

### Sync All Changes from Catalog

```bash
# Preview changes
kelp sync-local-catalog --target prod --dry-run

# Apply changes
kelp sync-local-catalog --target prod --output sync_summary.log
```

### Sync Specific Objects

```bash
# Sync a single table
kelp sync-local-catalog "customers" --target prod

# Sync a metric view
kelp sync-local-catalog "analytics.metrics.customer_agg" --target prod
```

## Error Handling

### Common Errors

**Configuration not found:**

```
✗ Project root with 'kelp_project.yml' not found...
```

**Solution:** Use `--config` to specify the path or cd to a directory containing `kelp_project.yml`.

**Target not found in configuration:**

```
✗ Target 'staging' not defined in kelp_project.yml
```

**Solution:** Add the target to `kelp_project.yml` or use a target that exists.

**Databricks profile not found:**

```
✗ Profile 'my-profile' not found in Databricks CLI
```

**Solution:** Configure the profile with `databricks configure` or use `-p` to specify an existing profile.

## See Also

- [Project Configuration](guides/project_config.md) - Detailed `kelp_project.yml` configuration
- [Sync Metadata with Your Catalog](guides/catalog.md) - Programmatic catalog sync
- [ODCS Integration](integrations/odcs.md) - Import/export with Open Data Contract Standard
