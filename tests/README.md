# Kelp Test Suite

This directory contains the test suite for the Kelp project, focusing on the `config` and `models` modules with emphasis on user-facing APIs.

## Structure

```
tests/
├── __init__.py
├── conftest.py                  # Global pytest fixtures and configuration
├── config/                      # Tests for kelp.config module
│   ├── __init__.py
│   ├── test_lifecycle.py       # Tests for init(), get_context(), ContextStore
│   └── test_project.py         # Tests for project config loading and resolution
├── models/                      # Tests for kelp.models module
│   ├── __init__.py
│   ├── test_catalog.py         # Tests for Catalog model
│   ├── test_metric_view.py     # Tests for MetricView model
│   └── test_table.py           # Tests for Table, Column, Constraints, Quality
└── fixtures/                    # Test project fixtures
    ├── minimal_project/         # Minimal project with no models
    ├── simple_project/          # Simple project with tables and metrics
    └── multi_target_project/    # Multi-target project (dev/prod)
```

## Test Fixtures

### Minimal Project
- Basic kelp_project.yml with no models
- Used for testing basic initialization

### Simple Project
- Contains example tables (customers, orders)
- Contains example metric view (customer_metrics)
- Uses variables for catalog and schema names
- Follows kelp conventions:
  - `kelp_models` root key in model files
  - `kelp_metric_views` root key in metric files
  - Jinja syntax: `${ variable }`
  - Hierarchy inheritance via `+catalog` and `+schema`

### Multi-Target Project
- Demonstrates target-specific configuration (dev/prod)
- Shows variable inheritance and override patterns
- Uses default vars that can be overridden by targets

## Running Tests

```bash
# Run all tests
uv run pytest tests/

# Run with verbose output
uv run pytest tests/ -v

# Run specific test module
uv run pytest tests/config/test_lifecycle.py

# Run specific test class
uv run pytest tests/models/test_table.py::TestTable

# Run specific test
uv run pytest tests/config/test_lifecycle.py::TestInit::test_init_minimal_project
```

## Key Features

- **Isolated tests**: Each test uses fixtures that are reset between runs
- **Context isolation**: Global context is cleared before/after each test
- **Real file fixtures**: Tests use actual YAML files following kelp conventions
- **User-facing APIs**: Focus on testing public APIs that users interact with
- **Comprehensive coverage**: Models, config loading, lifecycle management

## Adding New Tests

1. Create test file in appropriate module directory
2. Use existing fixtures or create new ones in `fixtures/`
3. Follow naming convention: `test_<module>.py`
4. Use descriptive test names: `test_<functionality>_<scenario>`
5. Add docstrings explaining what each test validates

## Conventions

- Use kelp syntax: `${ variable }` not `{{ variable }}`
- Model files use `kelp_models` root key
- Metric files use `kelp_metric_views` root key
- Catalog/schema inheritance via `+catalog`/`+schema` in hierarchy
- Variables resolved: init_vars > target vars > default vars
