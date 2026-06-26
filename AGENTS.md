# Laktory — AI Agent Reference

## What is Laktory

Laktory is a DataOps and ETL framework for building lakehouses on Databricks. Pipelines
and resources are declared as YAML, deployed via Terraform, and executed on Apache Spark
or Polars. Transformations are written as SQL expressions or Python functions using the
Narwhals DataFrame abstraction layer, enabling backend-agnostic logic that runs on both
Spark and Polars.

---

## Core YAML Concepts

### Custom tags

| Tag | Effect |
|-----|--------|
| `!use <path>` | Replace node with the full contents of `<path>` (relative to the current file) |
| `<<: !update <path>` | Deep-merge the mapping at `<path>` into the current mapping |
| `!extend <path>` | Append items from `<path>` into the current list |

### Variable placeholders

| Syntax | When to use |
|--------|-------------|
| `${vars.name}` | Simple string substitution; resolved at deploy time from stack variables |
| `${vars.NAME}` | Upper-case name refers to an environment/system variable |
| `${{ expr }}` | Python expression — evaluated at deploy time; supports conditionals |

Examples:
```yaml
catalog_name: ${vars.catalog_write}
num_workers: ${{ 4 if vars.env == 'prod' else 1 }}
suffix: ${{ '[${vars.USERNAME}]' if vars.catalog_write == 'sandbox' else '' }}
```

---

## Model Hierarchy

```
Stack
  └── resources
        ├── pipelines
        │     └── Pipeline
        │           ├── orchestrator
        │           │     └── LakeflowJobOrchestrator
        │           │         | LakeflowDeclarativePipelineOrchestrator
        │           │         | SparkDeclarativePipelineOrchestrator
        │           │         | AirflowOrchestrator
        │           └── nodes[]
        │                 └── PipelineNode
        │                       ├── sources[]
        │                       │     └── UnityCatalogDataSource
        │                       │         | FileDataSource
        │                       │         | PipelineNodeDataSource
        │                       │         | HiveMetastoreDataSource
        │                       ├── sinks[]
        │                       │     └── UnityCatalogDataSink
        │                       │         | FileDataSink
        │                       │         | PipelineViewDataSink
        │                       │         | HiveMetastoreDataSink
        │                       └── transformer
        │                             └── DataFrameTransformer
        │                                   └── nodes[]
        │                                         └── DataFrameExpr (SQL)
        │                                             | DataFrameMethod (func call)
        └── databricks_jobs, databricks_dashboards, etc.
```

A `Pipeline` can be used standalone (no `Stack`) when deploying only pipeline resources.

---

## Key Model Field Reference

### `PipelineNode`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Unique node name within the pipeline |
| `primary_keys` | `list[str]` | `null` | Columns that uniquely identify each row; used as default keys for CDC merge operations |
| `time_column` | `str` | `null` | Timestamp/temporal dimension column; enables time-aware joins and incremental processing |
| `sources` | `list[...]` | `[]` | Data sources. First entry is the primary input fed into the transformer as `{df}`; assign `name` to each to reference as `{sources.name}` |
| `sinks` | `list[...]` | `[]` | Data sink(s). Set `is_quarantine: true` to store expectation-failed rows |
| `transformer` | `DataFrameTransformer` | `null` | Chain of SQL or method transformations applied between source and sink |
| `execution_task_name` | `str` | `null` | Task group name for multi-task orchestrators (Databricks Jobs, Airflow). Nodes sharing the same name run in one task |
| `dataframe_api` | `NARWHALS \| NATIVE` | `NARWHALS` | DataFrame API used in transformer nodes. `NARWHALS` is backend-agnostic; `NATIVE` exposes backend-specific API (PySpark / Polars) |
| `depends_on` | `list[str]` | `[]` | Node names that must complete before this node, even when no data flows between them |
| `expectations` | `list[...]` | `[]` | Data quality expectations; can warn, drop invalid rows, or fail the pipeline |
| `ldp_template` | `str` | `DEFAULT` | Notebook template to use when Lakeflow Declarative Pipeline orchestrator is selected |
| `tags` | `list[str]` | `[]` | Tags for selective node execution |
| `comment` | `str` | `null` | Comment written to the associated table or view |

---

### `UnityCatalogDataSource`

Inherits common fields from `BaseDataSource` (see below).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog_name` | `str` | `null` | Source table catalog |
| `schema_name` | `str` | `null` | Source table schema |
| `table_name` | `str` | required | Source table name; supports fully qualified `catalog.schema.table` |
| `as_stream` | `bool` | `false` | Read as Spark Structured Streaming DataFrame |
| `reader_kwargs` | `dict` | `{}` | Options passed to the Spark reader (`.options(...)`) |
| `name` | `str` | `null` | Source alias; referenced as `{sources.name}` in transformer expressions |
| `filter` | `str` | `null` | SQL `WHERE` expression applied after read |
| `selects` | `list[str] \| dict[str, str]` | `null` | Columns to select (list) or select-and-rename (dict) |
| `renames` | `dict[str, str]` | `null` | Column rename mapping applied after read |
| `drop_duplicates` | `bool \| list[str]` | `null` | Remove duplicate rows; pass a list to deduplicate on specific columns |

---

### `FileDataSource`

Inherits common fields from `BaseDataSource`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `str` | required | File path on local disk, remote storage, or Databricks volume |
| `format` | `str` | required | File format: `AVRO`, `CSV`, `DELTA`, `JSON`, `JSONL`, `PARQUET`, `XML`, etc. |
| `as_stream` | `bool` | `false` | Read as stream (uses Databricks Auto Loader for cloud storage) |
| `has_header` | `bool` | `true` | First row is a header (CSV only) |
| `infer_schema` | `bool` | `false` | Infer column types from data (CSV, JSON) |
| `reader_kwargs` | `dict` | `{}` | Options passed directly to the backend reader |
| `schema_definition` | `DataFrameSchema` | `null` | Explicit schema (YAML alias: `schema`) |
| `name` | `str` | `null` | Source alias; referenced as `{sources.name}` |

---

### `PipelineNodeDataSource`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_name` | `str` | required | Name of the upstream `PipelineNode` whose output to read |
| `as_stream` | `bool` | `false` | Read the upstream node's sink as a streaming DataFrame |
| `reader_kwargs` | `dict` | `{}` | Options passed to the backend reader |

---

### `UnityCatalogDataSink`

Inherits common fields from `BaseDataSink`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog_name` | `str` | `null` | Target table catalog |
| `schema_name` | `str` | `null` | Target table schema |
| `table_name` | `str` | required | Target table name; supports fully qualified `catalog.schema.table` |
| `table_type` | `TABLE \| VIEW` | `TABLE` | Write a materialized table or a SQL view |
| `mode` | `str` | `null` | Write mode: `OVERWRITE`, `APPEND`, `MERGE`, `ERROR`, `IGNORE` |
| `format` | `DELTA \| PARQUET \| ORC \| AVRO` | `DELTA` | Storage format |
| `merge_cdc_options` | `DataSinkMergeCDCOptions` | `null` | CDC merge configuration (required when `mode: MERGE`) |
| `databricks_data_profiling_config` | `...` | `null` | Automatically creates a Databricks Data Quality Monitor on this table |
| `is_quarantine` | `bool` | `false` | Store rows that fail `expectations` instead of the main output |
| `checkpoint_path` | `str` | `null` | Checkpoint directory for streaming writes |

---

### `DataSinkMergeCDCOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `scd_type` | `1 \| 2` | `1` | SCD type: 1 = upsert in place, 2 = keep history with start/end timestamps |
| `primary_keys` | `list[str]` | `null` | Columns that identify a unique record in the source |
| `order_by` | `str` | `null` | Column used to sequence out-of-order CDC events; required for SCD type 2 |
| `delete_where` | `str` | `null` | SQL expression — when true, treat the CDC event as a DELETE |
| `start_at_column_name` | `str` | `__start_at` | SCD 2: column holding the row's effective-start timestamp |
| `end_at_column_name` | `str` | `__end_at` | SCD 2: column holding the row's effective-end timestamp |
| `ignore_null_updates` | `bool` | `false` | When true, null values in an update do not overwrite existing non-null values |
| `null_equals_null` | `bool` | `false` | Use null-safe equality (`<=>`) for primary key matching |
| `include_columns` | `list[str]` | `null` | Subset of columns to include in the target; mutually exclusive with `exclude_columns` |
| `exclude_columns` | `list[str]` | `null` | Columns to exclude from the target |

---

### `DataFrameTransformer`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `nodes` | `list[DataFrameExpr \| DataFrameMethod]` | required | Ordered list of transformation steps applied sequentially |

Each step is either a SQL expression (`expr`) or a method call (`func_name`). Mixing both in the same `nodes` list is supported.

**`DataFrameExpr` fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expr` | `str` | required | SQL `SELECT` statement. Use `{df}` for the primary source, `{sources.name}` for a named source |

**`DataFrameMethod` fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `func_name` | `str` | required | DataFrame method or function name (e.g. `drop_duplicates`, `select`, `mypackage.my_func`) |
| `func_args` | `list` | `[]` | Positional arguments; use `{df}`, `{sources.name}`, or `{nodes.X}` as DataFrame references |
| `func_kwargs` | `dict` | `{}` | Keyword arguments; same reference syntax as `func_args` |
| `dataframe_api` | `NARWHALS \| NATIVE` | inherited | Override the DataFrame API for this step only |

> When `func_name` contains a dot (e.g. `mypackage.my_func`), Laktory imports the
> function as `from mypackage import my_func` and calls it with the DataFrame as the
> first argument.

---

### `LakeflowJobOrchestrator`

Generates a Databricks Lakeflow Job that executes all pipeline nodes as tasks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | `null` | Job name (auto-derived from pipeline name if omitted) |
| `timeout_seconds` | `int` | `null` | Job-level timeout |
| `schedule` | `JobSchedule` | `null` | Cron-based schedule: `{ quartz_cron_expression, timezone_id }` |
| `serverless_environment_version` | `str` | `null` | Pin to a specific Databricks serverless runtime |
| `node_max_retries` | `int` | `null` | Max retries per node task on failure |
| `access_controls` | `list[AccessControl]` | `null` | RBAC permissions (`{ group_name, permission_level }`) |
| `tags` | `dict[str, str]` | `null` | Key-value tags attached to the Databricks Job |
| `webhook_notifications` | `JobWebhookNotifications` | `null` | Webhook alerts on job start, success, or failure |
| `job_clusters` | `list[JobCluster]` | `null` | Reusable cluster definitions referenced by tasks |
| `data_profiling_config_task` | `bool` | `false` | Append a post-execute task to update data quality monitor configs |

---

## Common YAML Patterns

### Bronze — streaming ingestion from files

```yaml
name: schema_brz_table
sources:
- path: /Volumes/${vars.catalog_read}/sources/input/path/
  format: CSV
  as_stream: true
sinks:
- catalog_name: ${vars.catalog_write}
  schema_name: schema
  table_name: brz_table
  mode: APPEND
transformer:
  nodes:
  - func_name: mypackage.brz_transform
```

### Silver — CDC merge from bronze node

```yaml
name: schema_slv_table
primary_keys: [KeyColumn]
time_column: tstamp
sources:
- node_name: schema_brz_table
  as_stream: true
sinks:
- catalog_name: ${vars.catalog_write}
  schema_name: schema
  table_name: slv_table
  mode: MERGE
  merge_cdc_options:
    scd_type: 1
    primary_keys: [KeyColumn]
    order_by: tstamp
transformer:
  nodes:
  - expr: !use ./slv_table.sql
```

### Gold — batch overwrite with custom function

```yaml
name: schema_gld_table
primary_keys: [KeyColumn]
sources:
- node_name: schema_slv_table
  as_stream: false
sinks:
- catalog_name: ${vars.catalog_write}
  schema_name: schema
  table_name: gld_table
  mode: OVERWRITE
transformer:
  nodes:
  - func_name: mypackage.my_gold_function
    func_kwargs:
      catalog_read: ${vars.catalog_read}
    dataframe_api: NARWHALS
```

### Scoped view — consumer-facing subset

```yaml
name: schema_gld_table_consumer
execution_task_name: views
sources:
- node_name: schema_gld_table
sinks:
- catalog_name: ${vars.catalog_write}
  schema_name: schema
  table_name: "`gld_table[consumer]`"
  table_type: VIEW
transformer:
  nodes:
  - expr: !use ./gld_table[consumer].sql
```

---

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Node name | `{schema}_{table_name}` | `sap_brz_cadran` |
| Table name | `snake_case` with layer prefix | `brz_cadran`, `slv_cadran`, `gld_cadran` |
| Schema name | `snake_case`, matches the domain folder | `sap`, `finance` |
| Table with brackets | Enclose in backticks in YAML | `` "`gld_table[consumer]`" `` |

Layer prefixes: `brz_` (bronze/raw), `slv_` (silver/cleansed), `gld_` (gold/business).

---

## Variable Injection

Variables are defined at the Stack level and overridden per environment:

```yaml
# stack.yaml
variables:
  env: dev
  catalog_write: dev
  catalog_read: raw

environments:
  prod:
    variables:
      env: prod
      catalog_write: prod
```

Inside YAML, three injection syntaxes are available:

```yaml
# Simple substitution
catalog_name: ${vars.catalog_write}

# Environment/system variable (upper-case name)
user_suffix: ${vars.USERNAME}

# Conditional expression
name_suffix: ${{ '[sandbox]' if vars.catalog_write == 'sandbox' else '' }}
```

Variables can also appear inside `!use` file paths:
```yaml
<<: !update ./overrides_${vars.env}.yaml
```

---

## MCP Server (if configured)

If a Laktory MCP server is configured in `.mcp.json`, prefer the MCP tools over this
static file — they return live, version-accurate documentation from the installed package:

| Tool | Use it to… |
|------|-----------|
| `get_model_docs` | Look up any model's fields before writing YAML |
| `validate_yaml` | Validate a YAML snippet before finalizing |
| `list_models` | Discover all available model names |
| `get_version` | Confirm the installed Laktory version |

Example `.mcp.json`:
```json
{
  "mcpServers": {
    "laktory": {
      "command": "python",
      "args": ["-m", "laktory.mcp.server"]
    }
  }
}
```

Install the MCP extra first: `pip install laktory[mcp]`
