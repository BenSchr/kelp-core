# kelp

Kelp is a catalog-based toolkit for Databricks Spark Declarative Pipelines (SDP) and native Spark ETL projects. It provides a simple api for catalog management, data quality and building reliable data pipelines. Kelp is designed to be flexible and extensible, allowing you to easily integrate it into your existing Spark projects. What distinguishes Kelp from other similar frameworks is its focus on beeing future-proof and easily maintainable againts any new features or syntax changes in the platform.

## Features
- SDP: Kelp provides simple apis and decorators to improve your pipeline code, quality and reliability. It also provides a simple way to build quarantine patterns based on the quality checks and expectations defined in the catalog.
- Data quality: Kelp provides the ability to define SDP expectations and DQX checks  
- Catalog management: Kelp provides a simple api for managing your catalog, including syncing it with your Spark metastore and refreshing it when needed. It also provides a simple way to define and manage your catalog in a yaml file. This includes descriptions, tags and table properties and other metadata that can be used for documentation and data discovery.
- Minimal invasive: Kelp is designed to be minimally invasive, allowing you to easily integrate it into your existing Spark projects without requiring significant changes to your codebase. To keep the project maintainable against the pace of new features and syntax changes in the platform.
