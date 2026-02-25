# kelp

Kelp is a catalog-based toolkit for Databricks Spark Declarative Pipelines (SDP) and native Spark ETL projects. It provides a simple api for catalog management, data quality and building reliable data pipelines. Kelp is designed to be flexible and extensible, allowing you to easily integrate it into your existing Spark projects. What distinguishes Kelp from other similar frameworks is its focus on beeing future-proof and easily maintainable againts any new features or syntax changes in the platform.

## Features
- SDP: Kelp provides simple apis and decorators to improve your pipeline code, quality and reliability. It also provides a simple way to build quarantine patterns based on the quality checks and expectations defined in the catalog.
- Data quality: Kelp provides the ability to define SDP expectations and DQX checks  
- Catalog management: Kelp provides a simple api for managing your catalog, including syncing it with your Spark metastore and refreshing it when needed. It also provides a simple way to define and manage your catalog in a yaml file. This includes descriptions, tags and table properties and other metadata that can be used for documentation and data discovery.
- Minimal invasive: Kelp is designed to be minimally invasive, allowing you to easily integrate it into your existing Spark projects without requiring significant changes to your codebase. To keep the project maintainable against the pace of new features and syntax changes in the platform.

# Features

- Easily manage your metadata
- Conveniently apply metadata to your pipelines and tables
- Build reliable pipelines with quality checks and quarantine patterns
- Apply tags, descriptions and other metadata to your tables for better documentation and data discovery
- Build for extensibility and future-proofing against new features and syntax changes without large refactors or breaking changes

## Variables and configuration management
Kelp provides a simple and flexible way to manage your metadata catalog.
- Specify enviromennt specific variables
- Apply metadata and settings hierarchically based on folder structure to keep your config dry and organized

## Spark Declarative Pipelines (SDP)
Kelp provides simple apis and decorators to improve your pipeline code, quality and reliability.
- Auto inject configuration, variables and schema from the metadata catalog
- Apply expectations and DQX checks defined in the metadata catalog
- Build quarantine patterns based on the quality checks and expectations defined in the catalog
- Use `ref` and `target` functions to auto-resolve table references and target tables based on the catalog definitions reducing the need for passing pipeline parameters
- Apply natively unsupported metadata like tags
- Apply metadata even if you omit the spark-schema from the pipeline table definition

## Metric Views
- Create and update Metric Views 
- Apply tags and descriptions to your metric views, dimensions and measures

## Native Spark
- Use the schema, properties and ddl to create and refine your tables in native Spark ETL projects
- Apply tags, descriptions and other metadata to your tables
- Use DQX checks defined in the catalog to apply data quality checks

## TODOs

- [x] Add pytest
- [ ] Add integration tests with Databricks 
- [ ] Add coverage
- [x] Add init command for project scaffolding
- [ ] Improve documentation with getting started and examples
- [x] Improve docstrings of user facing functions and classes
- [ ] Add transformations for "apply_schema" and "apply_dqx"
