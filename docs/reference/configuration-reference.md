# Configuration Reference

This page summarizes the most important `kelp_project.yml` keys.

## Project Paths

```yaml
kelp_project:
	models_path: "./kelp_metadata/models"
	metrics_path: "./kelp_metadata/metrics"
	functions_path: "./kelp_metadata/functions"
	abacs_path: "./kelp_metadata/abacs"
```

## Inheritance Blocks

Kelp supports hierarchical defaults via `+` keys.

```yaml
kelp_project:
	models:
		+catalog: ${ data_catalog }
		+schema: ${ data_schema }

	metric_views:
		+catalog: ${ data_catalog }
		+schema: ${ metric_schema }

	functions:
		+catalog: ${ function_catalog }
		+schema: ${ function_schema }

	abacs: {}
```

## Variables and Targets

```yaml
vars:
	data_catalog: analytics
	data_schema: core
	function_catalog: security
	function_schema: udf

targets:
	dev:
		vars:
			data_catalog: analytics_dev
			data_schema: core_dev
			function_catalog: security_dev
			function_schema: udf_dev
	prod:
		vars:
			data_catalog: analytics_prod
			data_schema: core_prod
			function_catalog: security_prod
			function_schema: udf_prod
```

## Object Root Keys

The following root keys are supported in metadata YAML files:

- `kelp_models`
- `kelp_metric_views`
- `kelp_functions`
- `kelp_abacs`

