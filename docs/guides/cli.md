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
uv run kelp version
```

Returns the Kelp banner and version information.

### `kelp validate`

Validate your Kelp project configuration and catalog.

```bash
uv run kelp validate \
  --config kelp_project.yml \
  --target prod \
  --debug
```

**Options:**

- `-c, --config` - Path to the `kelp_project.yml` file (optional, auto-discovers if not provided)
- `--target` - Environment to validate against (e.g., `dev`, `prod`)
- `--debug` - Enable debug logging

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

### `kelp json-schema`

Generate a JSON schema for `kelp_project.yml` configuration validation.

```bash
uv run kelp json-schema \
  --output kelp_json_schema.json \
  --dry-run
```

### `kelp check-policies`

Evaluate metadata governance policies against your local table catalog.

```bash
uv run kelp check-policies \
  --config kelp_project.yml \
  --target prod
```

**Options:**

- `-c, --config` - Path to the `kelp_project.yml` file (optional, auto-discovers if not provided)
- `--target` - Environment to check against (e.g., `dev`, `prod`)
- `--severity` - Only display violations at this severity or above: `warn` or `error` (default: all)
- `--fail-on` - Exit with code 1 when violations of this severity are found: `warn` or `error` (default: `error`)
- `--debug` - Enable debug logging

> **Note:** Policies must be enabled in your project settings for this command to run checks.
> Set `policy_config.enabled: true` in `kelp_project.yml`.

**Output when policies are disabled:**

```
⚠ Policy checks are disabled. Set 'policy_config.enabled: true' in kelp_project.yml to enable.
```

**Output with violations:**

```
⚠ [WARN]  catalog.schema.bronze_customers — Table 'catalog.schema.bronze_customers' is missing a description.
✗ [ERROR] catalog.schema.silver_orders.order_id — Column 'order_id' in table '...' is missing a description.

Policy check complete: 1 error(s), 1 warning(s) across 15 table(s).
```

**Output when all checks pass:**

```
✓ Policy check complete: no violations found (15 table(s) checked).
```

**Use in CI/CD:**

```bash
# Fail the pipeline on any policy error
uv run kelp check-policies --target prod --fail-on error
```

**Options:**

- `-o, --output` - Output path for the JSON schema file (default: `kelp_json_schema.json`)
- `--dry-run` - Preview output without writing to file

**Use Case:**

Import the generated JSON schema into your IDE for YAML validation and autocomplete:

```yaml
# yaml-language-server: $schema=./kelp_json_schema.json
kelp_project:
  models_path: "./kelp_metadata/models"
  # ... rest of configuration ...
```

### `kelp sync-from-catalog`

Fetch table metadata from Databricks Unity Catalog and generate a YAML model definition.

```bash
uv run kelp sync-from-catalog \
  "analytics_prod.core.customers" \
  --profile my-profile \
  --output customers.yml \
  --dry-run
```

**Arguments:**

- `table_path` - Fully qualified table name (required), e.g., `catalog.schema.table`

**Options:**

- `-p, --profile` - Databricks CLI profile to use
- `-o, --output` - Path to output YAML file (optional - prints to stdout if not provided)
- `--dry-run` - Preview output without writing
- `--debug` - Enable debug logging

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
uv run kelp sync-from-pipeline \
  --id abc123def456 \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output sync_report.log
```

**Options:**

- `--id` - Databricks pipeline ID to sync from (optional if auto-detected from config)
- `-c, --config` - Path to `kelp_project.yml` (auto-detects if not provided)
- `--target` - Environment for variable resolution
- `-p, --profile` - Databricks CLI profile
- `-o, --output` - Path to output sync report log
- `--dry-run` - Preview changes without writing
- `--debug` - Enable debug logging

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
uv run kelp sync-local-catalog \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output sync_report.log \
  --dry-run
```

Sync a specific object by name:

```bash
uv run kelp sync-local-catalog \
  "customers" \
  --config kelp_project.yml \
  --target prod
```

**Arguments:**

- `[name]` - Optional table or metric view name/FQN to sync (optional - syncs all if not provided)

**Options:**

- `-c, --config` - Path to `kelp_project.yml`
- `--target` - Environment to sync against
- `-p, --profile` - Databricks CLI profile
- `-o, --output` - Path to output sync log file
- `--dry-run` - Preview changes without writing
- `--debug` - Enable debug logging

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
uv run kelp generate-ddl \
  --config kelp_project.yml \
  --target prod \
  --output create_tables.sql \
  --dry-run
```

**Options:**

- `-c, --config` - Path to `kelp_project.yml` (auto-detects if not provided)
- `--target` - Environment for variable resolution
- `-o, --output` - Path to output SQL file (optional - prints to stdout if not provided)
- `--dry-run` - Preview output without writing
- `--debug` - Enable debug logging

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
uv run kelp generate-alter-statements \
  --config kelp_project.yml \
  --target prod \
  --profile my-profile \
  --output alter_tables.sql \
  --dry-run
```

**Options:**

- `-c, --config` - Path to `kelp_project.yml` (auto-detects if not provided)
- `--target` - Environment for variable resolution
- `-p, --profile` - Databricks CLI profile for table introspection
- `-o, --output` - Path to output SQL file (optional - prints to stdout if not provided)
- `--dry-run` - Preview output without writing
- `--debug` - Enable debug logging

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
uv run kelp init .
```

**Creates:**

- `kelp_project.yml` - Base project configuration
- `kelp_metadata/models/` - Directory for model definitions
- `kelp_metadata/metrics/` - Directory for metric view definitions
- `kelp_metadata/functions/` - Directory for function definitions (optional)
- `.gitkeep` files for git tracking

## Configuration Environment Variables

Configure Kelp CLI behavior using environment variables:

### `KELP_PROJECT_FILE`

Path to the `kelp_project.yml` file (overrides auto-discovery):

```bash
export KELP_PROJECT_FILE="./custom_config.yml"
uv run kelp validate
```

### `KELP_TARGET`

Default environment target:

```bash
export KELP_TARGET="prod"
uv run kelp validate  # Uses "prod" if not overridden by --target
```

### `KELP_PROFILE`

Default Databricks CLI profile:

```bash
export KELP_PROFILE="my-workspace"
uv run kelp sync-from-catalog "analytics.core.customers"
```

## Common Workflows

### Setup New Project

```bash
# Create project structure
uv run kelp init project --path ./my_project --catalog my_catalog

# Generate JSON schema for IDE support
cd my_project
uv run kelp json-schema

# Validate configuration
uv run kelp validate
```

### Import Existing Table Metadata

```bash
# Fetch from Databricks and generate YAML
uv run kelp sync-from-catalog \
  "analytics_prod.core.customers" \
  -p my-profile \
  -o kelp_metadata/models/customers.yml

# Validate the generated file
uv run kelp validate
```

### Validate Before Deployment

```bash
# Validate development environment
uv run kelp validate --target dev

# Validate production environment
uv run kelp validate --target prod
```

### Sync All Changes from Catalog

```bash
# Preview changes
uv run kelp sync-local-catalog --target prod --dry-run

# Apply changes
uv run kelp sync-local-catalog --target prod --output sync_summary.log
```

### Sync Specific Objects

```bash
# Sync a single table
uv run kelp sync-local-catalog "customers" --target prod

# Sync a metric view
uv run kelp sync-local-catalog "analytics.metrics.customer_agg" --target prod
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

## Best Practices

1. **Use `--dry-run` before applying changes** - Always preview with `--dry-run` before syncing or making changes.

2. **Store JSON schema in version control** - Commit `kelp_json_schema.json` to enable IDE autocomplete for all team members.

3. **Validate on deploy** - Include `kelp validate` in your deployment/CI pipeline.

4. **Use target-specific validation** - Validate with the target environment to catch configuration issues early.

5. **Keep seeds in git** - Commit initial model YAMLs after first import, then update with `sync-local-catalog`.

6. **Profile management** - Use consistent Databricks CLI profiles across your team (e.g., `work-prod`, `work-dev`).

## See Also

- [Project Configuration](project_config.md) - Detailed `kelp_project.yml` configuration
- [Sync Metadata with Your Catalog](catalog.md) - Programmatic catalog sync
- Getting Started Guide - Step-by-step setup instructions
