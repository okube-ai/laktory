# Laktory

## Project Overview

Laktory is a DataOps and ETL framework for building lakehouses on Databricks. It combines:
- **Data pipeline definitions** as code (transformations, sources, sinks)
- **Infrastructure-as-Code** for Databricks resources (Unity Catalog, compute, access grants)
- **Multi-backend dataframe support** via Narwhals (Spark and Polars)
- **Multiple orchestrator support** (local, Databricks Jobs/DLT Pipelines, Apache Airflow)

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Data modeling | Pydantic v2 |
| DataFrame abstraction | Narwhals |
| DataFrame backends | Apache Spark (PySpark), Polars |
| SQL parsing | SQLGlot |
| IaC backends | Pulumi, Terraform |
| CLI | Typer |
| DAG management | NetworkX |

## Key Directories

| Path | Purpose |
|------|---------|
| `laktory/models/` | All Pydantic models — the core of the library |
| `laktory/models/resources/databricks/` | 50+ Databricks resource definitions (Catalog, Job, Cluster, etc.) |
| `laktory/models/pipeline/` | Pipeline, PipelineNode, and execution plan |
| `laktory/models/datasources/` | Input sources (file, Unity Catalog, Hive, DataFrame, node) |
| `laktory/models/datasinks/` | Output sinks (file, Unity Catalog, Hive, pipeline view) |
| `laktory/models/dataframe/` | Transformation chain: DataFrameTransformer, DataFrameMethod, DataFrameExpr |
| `laktory/models/stacks/` | Stack composition (Stack, StackResources, PulumiStack, TerraformStack) |
| `laktory/models/grants/` | Access control grant models |
| `laktory/narwhals_ext/` | Custom Narwhals extensions (dataframe, expr, functions) |
| `laktory/yaml/` | YAML loader with custom tags (`!use`, `!extend`, `!update`) |
| `laktory/cli/` | CLI commands (`laktory run`, `laktory validate`, etc.) |
| `laktory/_testing/` | Test helpers and fixture utilities |
| `tests/` | Pytest test suite |

## Build & Test Commands

```bash
# Install
uv sync                  # base dependencies
uv sync --all-extras     # all extras (dev, polars, pyspark, databricks, airflow)

# Code quality
ruff format ./
ruff check ./

# Tests (excludes Databricks Connect tests)
make test
# or directly:
uv run pytest -m "not databricks_connect" --cov=laktory tests

# Build & publish
uv build
uv publish
```

## Additional Documentation

Check these files when working on related topics:

| File | When to consult |
|------|----------------|
| `.claude/docs/architectural_patterns.md` | Model hierarchy, resource patterns, pipeline composition, variable injection, YAML tags |
| `.claude/docs/strategic_roadmap.md` | Planned direction: dropping Pulumi, DABs integration, Terraform schema generation, scope decisions |
