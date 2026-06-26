# Laktory — AI Agent Reference

> **Audience:** Data engineers using Laktory to build lakehouses on Databricks.
> Contributors developing Laktory itself should refer to `CLAUDE.md` at the repo root.

## What is Laktory

Laktory is a DataOps and ETL framework for building lakehouses on Databricks. Pipelines,
catalogs, users, groups, clusters, warehouses, and all other Databricks resources are
declared as YAML and deployed via Terraform or Databricks Asset Bundles (DABs).
Transformations are written as SQL expressions or Python functions using the Narwhals
DataFrame abstraction layer, enabling backend-agnostic logic that runs on both Spark and
Polars. The result is a single source of truth for both infrastructure and data pipelines
that can be version-controlled and CI/CD-deployed.

---

## Core YAML Concepts

### Composition tags

| Tag | Effect |
|-----|--------|
| `!use <path>` | Replace node with the full contents of another file |
| `<<: !update <path>` | Deep-merge another file's mapping into the current mapping |
| `!extend <path>` | Append another file's list items into the current list |

All paths are relative to the current file. Variables can appear in paths:
`!use catalogs_${vars.env}.yaml`

### Variable placeholders

| Syntax | Description |
|--------|-------------|
| `${vars.name}` | String substitution; resolved at deploy time from stack variables |
| `${vars.NAME}` | Upper-case name reads from environment / system variables |
| `${{ expr }}` | Python expression — evaluated at deploy time; supports conditionals |
| `${resources.<key>.<prop>}` | Cross-reference another resource's runtime output (e.g. `.id`, `.name`) |

```yaml
catalog_name: ${vars.catalog}
isolation_mode: ${{ 'ISOLATED' if vars.env == 'prd' else 'OPEN' }}
group_ids:
  - ${resources.group-role-engineers.id}
```

---

## Stack Structure

A `Stack` is the top-level deployment unit. Top-level fields:

```yaml
name: my-stack
description: Short description
backend: terraform          # only "terraform" is supported

resources:
  providers: ...            # Terraform provider configs
  pipelines: ...            # Laktory Pipeline definitions
  databricks_catalogs: ...  # Unity Catalog resources
  databricks_groups: ...
  # ... 50+ resource types

variables:                  # stack-level defaults
  env: dev
  catalog: dev

environments:               # per-environment variable overrides
  dev:
    variables:
      env: dev
      catalog: o3_dev
  prd:
    variables:
      env: prd
      catalog: o3_prd
```

### Multi-provider setup

Databricks has two provider scopes: **account-level** (groups, users, metastore) and
**workspace-level** (catalogs, pipelines, jobs). Both can be declared in one stack using
provider aliases:

```yaml
resources:
  providers:
    databricks:                       # account-level provider
      host: https://accounts.azuredatabricks.net
      account_id: ${vars.DATABRICKS_ACCOUNT_ID}
      azure_tenant_id: ${vars.AZURE_TENANT_ID}
      azure_client_id: ${vars.AZURE_CLIENT_ID}
      azure_client_secret: ${vars.AZURE_CLIENT_SECRET}
      auth_type: azure-client-secret

    databricks.dev:                   # workspace-level alias for dev
      alias: dev
      host: ${vars.DATABRICKS_HOST}
      token: ${vars.DATABRICKS_TOKEN}
```

Resources that need a specific provider set `resource_options.provider`:
```yaml
catalog-dev:
  name: dev
  resource_options:
    provider: ${resources.databricks.dev}
```

---

## Model Hierarchy

```
Stack
  ├── resources
  │     ├── pipelines
  │     │     └── Pipeline
  │     │           ├── orchestrator
  │     │           │     └── LakeflowJobOrchestrator
  │     │           │         | LakeflowDeclarativePipelineOrchestrator
  │     │           │         | SparkDeclarativePipelineOrchestrator
  │     │           │         | AirflowOrchestrator
  │     │           └── nodes[]
  │     │                 └── PipelineNode
  │     │                       ├── sources[]
  │     │                       │     └── UnityCatalogDataSource
  │     │                       │         | FileDataSource
  │     │                       │         | PipelineNodeDataSource
  │     │                       │         | HiveMetastoreDataSource
  │     │                       ├── sinks[]
  │     │                       │     └── UnityCatalogDataSink
  │     │                       │         | FileDataSink
  │     │                       │         | PipelineViewDataSink
  │     │                       └── transformer
  │     │                             └── DataFrameTransformer
  │     │                                   └── nodes[]
  │     │                                         └── DataFrameExpr (SQL)
  │     │                                             | DataFrameMethod (func call)
  │     │
  │     ├── databricks_catalogs → Catalog
  │     │     └── schemas[] → Schema
  │     │           └── volumes[] → Volume
  │     │
  │     ├── databricks_groups → Group
  │     ├── databricks_users → User
  │     ├── databricks_serviceprincipals → ServicePrincipal
  │     ├── databricks_metastores → Metastore
  │     ├── databricks_storagecredentials → StorageCredential
  │     ├── databricks_externallocations → ExternalLocation
  │     ├── databricks_warehouses → SqlWarehouse
  │     ├── databricks_clusters → Cluster
  │     ├── databricks_clusterpolicies → ClusterPolicy
  │     ├── databricks_secretscopes → SecretScope (with inline secrets[])
  │     ├── databricks_jobs → Job (standalone, not a pipeline orchestrator)
  │     ├── databricks_workspacetrees → WorkspaceTree
  │     ├── databricks_pythonpackages → PythonPackage
  │     └── ... (50+ resource types total)
  │
  └── providers → Terraform provider configuration
```

`Pipeline` can also be used standalone (without a `Stack`) when only pipeline resources are needed.

---

## Key Model Field Reference

### `PipelineNode`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Unique node name within the pipeline |
| `primary_keys` | `list[str]` | `null` | Columns that uniquely identify each row; used as default keys for CDC merge |
| `time_column` | `str` | `null` | Timestamp column; enables time-aware operations |
| `sources` | `list[...]` | `[]` | Data sources. First entry is the primary (`{df}`); assign `name` to reference as `{sources.name}` |
| `sinks` | `list[...]` | `[]` | Data sinks. Set `is_quarantine: true` to store expectation-failed rows |
| `transformer` | `DataFrameTransformer` | `null` | Chain of SQL / method transformations |
| `execution_task_name` | `str` | `null` | Groups nodes into one task in Databricks Jobs / Airflow |
| `dataframe_api` | `NARWHALS \| NATIVE` | `NARWHALS` | API used in transformer nodes. `NATIVE` exposes backend-specific API |
| `depends_on` | `list[str]` | `[]` | Node names to wait for even when no data flows between them |
| `expectations` | `list[...]` | `[]` | Data quality checks: warn, drop, quarantine, or fail |
| `ldp_template` | `str` | `DEFAULT` | Notebook template for Lakeflow Declarative Pipeline orchestrator |
| `tags` | `list[str]` | `[]` | Tags for selective execution |
| `comment` | `str` | `null` | Comment written to the associated table or view |

---

### `UnityCatalogDataSource`

Inherits common fields from `BaseDataSource`.

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
| `renames` | `dict[str, str]` | `null` | Column rename mapping |
| `drop_duplicates` | `bool \| list[str]` | `null` | Remove duplicates; pass a list to deduplicate on specific columns |

---

### `FileDataSource`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | `str` | required | File path on local disk, remote storage, or Databricks volume |
| `format` | `str` | required | `AVRO`, `CSV`, `DELTA`, `JSON`, `JSONL`, `PARQUET`, `XML`, etc. |
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

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog_name` | `str` | `null` | Target table catalog |
| `schema_name` | `str` | `null` | Target table schema |
| `table_name` | `str` | required | Target table name; supports fully qualified `catalog.schema.table` |
| `table_type` | `TABLE \| VIEW` | `TABLE` | Write a materialized table or a SQL view |
| `mode` | `str` | `null` | `OVERWRITE`, `APPEND`, `MERGE`, `ERROR`, `IGNORE` |
| `format` | `DELTA \| PARQUET \| ORC \| AVRO` | `DELTA` | Storage format |
| `merge_cdc_options` | `DataSinkMergeCDCOptions` | `null` | CDC merge config; required when `mode: MERGE` |
| `databricks_data_profiling_config` | `...` | `null` | Automatically creates a Databricks Data Quality Monitor on this table |
| `is_quarantine` | `bool` | `false` | Stores rows that fail `expectations` |
| `checkpoint_path` | `str` | `null` | Checkpoint directory for streaming writes |
| `metadata` | `TableDataSinkMetadata` | `null` | Table/column-level comments, tags, and Delta properties |

---

### `DataSinkMergeCDCOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `scd_type` | `1 \| 2` | `1` | SCD type: 1 = upsert in place, 2 = keep history with start/end timestamps |
| `primary_keys` | `list[str]` | `null` | Columns that identify a unique record in the source |
| `order_by` | `str` | `null` | Column used to sequence out-of-order CDC events; required for SCD 2 |
| `delete_where` | `str` | `null` | SQL expression — when true, treat the CDC event as a DELETE |
| `start_at_column_name` | `str` | `__start_at` | SCD 2: effective-start timestamp column |
| `end_at_column_name` | `str` | `__end_at` | SCD 2: effective-end timestamp column |
| `ignore_null_updates` | `bool` | `false` | Null values in an update do not overwrite existing non-null values |
| `null_equals_null` | `bool` | `false` | Use null-safe equality (`<=>`) for primary key matching |
| `include_columns` | `list[str]` | `null` | Subset of columns to include; mutually exclusive with `exclude_columns` |
| `exclude_columns` | `list[str]` | `null` | Columns to exclude from the target |

---

### `DataFrameTransformer`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `nodes` | `list[DataFrameExpr \| DataFrameMethod]` | required | Ordered list of transformation steps applied sequentially |

**`DataFrameExpr` fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expr` | `str` | required | SQL `SELECT` statement; use `{df}` for primary source, `{sources.name}` for named source |

**`DataFrameMethod` fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `func_name` | `str` | required | Method, attribute, or importable function (e.g. `drop_duplicates`, `mypackage.my_func`) |
| `func_args` | `list` | `[]` | Positional args; use `{df}`, `{sources.name}`, `{nodes.X}` as DataFrame references |
| `func_kwargs` | `dict` | `{}` | Keyword args; same reference syntax as `func_args` |
| `dataframe_api` | `NARWHALS \| NATIVE` | inherited | Override the DataFrame API for this step only |

> When `func_name` contains a dot (e.g. `mypackage.my_func`), Laktory imports it as
> `from mypackage import my_func` and calls it with the DataFrame as the first argument.

---

### `LakeflowJobOrchestrator`

Generates a Databricks Lakeflow Job that runs all pipeline nodes as tasks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | `null` | Job name (auto-derived from pipeline name if omitted) |
| `timeout_seconds` | `int` | `null` | Job-level timeout |
| `schedule` | `JobSchedule` | `null` | Cron schedule: `{ quartz_cron_expression, timezone_id, pause_status }` |
| `serverless_environment_version` | `str` | `null` | Pin to a specific Databricks serverless runtime version |
| `node_max_retries` | `int` | `null` | Max retries per node task on failure |
| `access_controls` | `list[AccessControl]` | `null` | RBAC permissions: `{ group_name, permission_level }` |
| `tags` | `dict[str, str]` | `null` | Key-value tags attached to the job |
| `webhook_notifications` | `JobWebhookNotifications` | `null` | Alerts on start, success, or failure |
| `job_clusters` | `list[JobCluster]` | `null` | Reusable cluster definitions |
| `data_profiling_config_task` | `bool` | `false` | Appends a post-execute task to update data quality monitor configs |

---

## Common Patterns

### Data Pipeline — Bronze (streaming from files)

```yaml
name: brz_stock_prices
sources:
- path: /Volumes/${vars.catalog}/sources/landing/events/yahoo-finance/stock_price/
  format: JSON
  as_stream: true
  reader_kwargs:
    cloudFiles.inferColumnTypes: true
sinks:
- schema_name: yahoo
  table_name: brz_stock_prices
```

### Data Pipeline — Silver (CDC merge with expectations)

```yaml
name: slv_stock_prices
sources:
- node_name: brz_stock_prices
  as_stream: true
expectations:
- name: positive_price
  expr: open > 0
  action: QUARANTINE
sinks:
- schema_name: yahoo
  table_name: slv_stock_prices
- schema_name: yahoo
  table_name: slv_stock_prices_quarantine
  is_quarantine: true
transformer:
  nodes:
  - expr: |
      SELECT
        data.created_at AS created_at,
        data.symbol AS symbol,
        data.open AS open,
        data.close AS close
      FROM {df}
  - func_name: drop_duplicates
    dataframe_api: NATIVE
    func_kwargs:
      subset: [symbol, created_at]
```

### Data Pipeline — Silver merge with SCD 1

```yaml
name: slv_stock_metadata
sources:
- node_name: brz_stock_metadata
  as_stream: true
sinks:
- schema_name: yahoo
  table_name: slv_stock_metadata
  mode: MERGE
  merge_cdc_options:
    scd_type: 1
    primary_keys: [symbol]
    order_by: extracted_at
```

### Data Pipeline — Gold (Narwhals aggregation)

```yaml
name: gld_stock_prices_by_1d
sources:
- node_name: slv_stocks
  as_stream: false
sinks:
- schema_name: yahoo
  table_name: gld_stock_prices_by_1d
  mode: OVERWRITE
transformer:
  dataframe_api: NARWHALS
  nodes:
  - func_name: with_columns
    func_kwargs:
      day_id: nw.col("created_at").dt.truncate("1d")
  - func_name: laktory.groupby_and_agg
    func_kwargs:
      groupby_columns: [symbol, day_id]
      agg_expressions:
      - expr: nw.col('low').min().alias("low")
      - expr: nw.col('high').max().alias("high")
      - expr: nw.col('close').max().alias("close")
```

### Data Pipeline — Multi-source join

```yaml
name: slv_stocks
sources:
- name: prices                        # primary source → {df} in transformer
  node_name: slv_stock_prices
  as_stream: false
- name: metadata                      # secondary source → {sources.metadata}
  node_name: slv_stock_metadata
  selects: [symbol, currency, first_traded]
sinks:
- schema_name: market
  table_name: slv_stocks
transformer:
  nodes:
  - func_name: join
    func_kwargs:
      other: "{sources.metadata}"
      "on": [symbol]
```

### Data Pipeline — Scoped view

```yaml
name: gld_stock_prices_consumer
execution_task_name: views
sources:
- node_name: gld_stock_prices_by_1d
sinks:
- schema_name: yahoo
  table_name: "`gld_stock_prices[consumer]`"
  table_type: VIEW
transformer:
  nodes:
  - expr: SELECT * FROM {df} WHERE symbol = 'AAPL'
```

### Data Pipeline — Pipeline with Lakeflow Job orchestrator

```yaml
name: pl-stock-prices

dependencies:
- laktory==0.12.x
- /Workspace/${vars.workspace_root}/wheels/lake-0.0.1-py3-none-any.whl

orchestrator:
  type: LAKEFLOW_JOB
  serverless_environment_version: "3"
  schedule:
    quartz_cron_expression: '0 0 0 ? * * *'
    timezone_id: UTC
    pause_status: ${vars.pause_status}
  access_controls:
  - group_name: account users
    permission_level: CAN_VIEW
  - group_name: role-engineers
    permission_level: CAN_MANAGE_RUN

nodes:
- name: brz_stock_prices
  ...
```

### Data Pipeline — Pipeline with Lakeflow Declarative Pipeline orchestrator

```yaml
name: pl-stock-prices

orchestrator:
  type: LAKEFLOW_DECLARATIVE_PIPELINE
  serverless: true
  catalog: ${vars.catalog}
  schema: yahoo
  development: ${vars.is_dev}
  libraries:
  - notebook:
      path: /.laktory/pipelines/laktory_ldp.py
  access_controls:
  - group_name: role-engineers
    permission_level: CAN_RUN

nodes:
- name: brz_stock_prices
  ...
```

---

### Unity Catalog — Catalog with schemas, volumes, and grants

```yaml
# stack.yaml
resources:
  databricks_catalogs:
    catalog-dev:
      name: dev
      isolation_mode: ${{ 'ISOLATED' if vars.env == 'prd' else 'OPEN' }}
      grants:
      - principal: account users
        privileges: [USE_CATALOG, USE_SCHEMA]
      - principal: role-metastore-admins
        privileges: [ALL_PRIVILEGES, MANAGE]
      schemas:
      - !extend schemas.yaml       # shared schema list
      - name: sources
        volumes:
        - name: landing
          volume_type: EXTERNAL
          storage_location: ${resources.external-location-landing.url}
          grants:
          - principal: account users
            privileges: [READ_VOLUME]
      resource_options:
        provider: ${resources.databricks.dev}
        is_enabled: true           # set to false to skip deploying this resource
```

```yaml
# schemas.yaml (shared list, extended by !extend)
- name: yahoo
  grants:
  - principal: domain-yahoo
    privileges: [SELECT]
- name: market
  grants:
  - principal: domain-market
    privileges: [SELECT]
- name: sandbox
  grants:
  - principal: account users
    privileges: [SELECT, MODIFY, CREATE_TABLE, CREATE_FUNCTION]
```

### Unity Catalog — Storage credentials and external locations

```yaml
resources:
  databricks_storagecredentials:
    storage-credentials-lakehouse:
      name: lakehouse-${vars.env}
      azure_managed_identity:
        access_connector_id: /subscriptions/.../accessConnectors/o3-dbksac-lakehouse-${vars.env}
      grants:
      - principal: role-metastore-admins
        privileges: [ALL_PRIVILEGES, MANAGE]

  databricks_externallocations:
    external-location-landing:
      name: external-location-landing-${vars.env}
      credential_name: ${resources.storage-credentials-lakehouse.name}
      url: abfss://landing@mystorageaccount${vars.env}.dfs.core.windows.net/
      grants:
      - principal: role-metastore-admins
        privileges: [ALL_PRIVILEGES, EXTERNAL_USE_LOCATION]
```

---

### Account — Groups, users, and service principals

```yaml
# groups.yaml
group-role-engineers:
  display_name: role-engineers
  workspace_permission_assignments:
  - workspace_id: ${vars.workspace_id_dev}
    permissions: [USER]
  - workspace_id: ${vars.workspace_id_prd}
    permissions: [USER]

group-role-workspace-admins:
  display_name: role-workspace-admins
  workspace_permission_assignments:
  - workspace_id: ${vars.workspace_id_dev}
    permissions: [ADMIN]
```

```yaml
# users.yaml
user-data-engineer:
  user_name: data.engineer@company.com
  group_ids:
  - ${resources.group-role-engineers.id}

# Reference an existing user without re-creating them
user-admin:
  lookup_existing:
    user_id: "7450234719887892"
  group_ids:
  - ${resources.group-role-workspace-admins.id}
```

```yaml
# serviceprincipals.yaml
service-principal-cicd-dev:
  lookup_existing:
    application_id: ${vars.SP_CLIENT_ID_DEV}
  group_ids:
  - ${resources.group-role-workspace-admins.id}
  roles:
  - account_admin
```

---

### Workspace — Warehouses, clusters, and secrets

```yaml
# warehouses.yaml
warehouse-default:
  name: default
  cluster_size: 2X-Small
  enable_serverless_compute: true
  auto_stop_mins: 10
  min_num_clusters: 1
  max_num_clusters: 2
  access_controls:
  - group_name: account users
    permission_level: CAN_USE
```

```yaml
# clusters.yaml
cluster-default:
  cluster_name: default
  spark_version: 17.2.x-scala2.13
  data_security_mode: USER_ISOLATION
  node_type_id: Standard_DS3_v2
  autoscale:
    min_workers: 1
    max_workers: 4
  autotermination_minutes: 30
  access_controls:
  - group_name: role-engineers
    permission_level: CAN_RESTART
  spark_env_vars:
    LAKTORY_WORKSPACE_ENV: ${vars.env}
```

```yaml
# secretscopes.yaml
secret-scope-azure:
  name: azure
  secrets:
  - key: tenant-id
    string_value: ${vars.AZURE_TENANT_ID}
  - key: client-id
    string_value: ${vars.AZURE_CLIENT_ID}
  permissions:
  - permission: READ
    principal: role-engineers
```

### Workspace — Standalone Databricks Job (not a pipeline orchestrator)

```yaml
# job-ingest.yaml
name: job-ingest-stock-prices

schedule:
  quartz_cron_expression: '0 0 6 ? * * *'
  timezone_id: UTC
  pause_status: ${vars.pause_status}

environments:
- environment_key: default
  spec:
    environment_version: "4"
    dependencies:
    - laktory==0.12.x
    - databricks-sdk>=0.114.0

tasks:
- task_key: ingest
  notebook_task:
    notebook_path: /.laktory/jobs/ingest_stock_prices.py

- task_key: pipeline
  depends_on:
  - task_key: ingest
  pipeline_task:
    pipeline_id: ${resources.pl-stock-prices.id}

access_controls:
- group_name: account users
  permission_level: CAN_VIEW
- group_name: role-engineers
  permission_level: CAN_MANAGE_RUN
```

---

### Databricks Asset Bundles (DABs) — Alternative to Terraform

DABs is a modern alternative to Terraform for deploying pipelines and jobs. Pipeline YAML
files stay identical; DABs discovers and deploys them automatically.

**`databricks.yml`:**
```yaml
bundle:
  name: my-lakehouse

artifacts:
  python_artifact:
    type: whl
    build: uv build --wheel

# Laktory discovers pipeline YAMLs and creates jobs/pipelines in Databricks
python:
  venv_path: .venv
  resources:
    - 'laktory.dab:build_resources'

sync:
  paths:
    - ./src
    - ./laktory
  include:
    - ./laktory/.build/**

variables:
  dab_workspace_root:           # required by Laktory
    default: ${workspace.root_path}
  laktory_pipelines_dir:        # where pipeline YAMLs live
    default: ./laktory/pipelines
  catalog:
    description: The target catalog

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    variables:
      catalog: dev
  prd:
    mode: production
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    variables:
      catalog: prd
```

Key differences from Terraform:
- `targets` (dev/prd) replace Stack `environments`
- Account-level resources (groups, metastore) still require Terraform
- Pipeline YAML does not need `orchestrator.type` — DABs creates the job automatically
- Use `databricks bundle deploy` instead of `laktory deploy`

---

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Table names | `snake_case` with layer prefix | `brz_stock_prices`, `slv_stock_prices`, `gld_stock_prices_by_1d` |
| Schema names | Domain noun, `snake_case` | `yahoo`, `finance`, `market`, `nyctaxi` |
| Table with brackets | Enclose entire name in backticks | `` "`gld_stock_prices[consumer]`" `` |

Layer prefixes: `brz_` (bronze/raw), `slv_` (silver/cleansed), `gld_` (gold/business-ready).

Node names, resource keys, and file names are project-specific — Laktory enforces no
particular pattern beyond uniqueness within a stack or pipeline.

---

## Variable Injection

Variables are defined at the Stack level and overridden per environment:

```yaml
variables:
  env: dev
  catalog: dev

environments:
  prd:
    variables:
      env: prd
      catalog: prd
```

Inside YAML, three injection syntaxes are available:

```yaml
# Simple substitution
catalog_name: ${vars.catalog}

# Environment / system variable (upper-case)
token: ${vars.DATABRICKS_TOKEN}

# Python expression — conditional or computed value
isolation_mode: ${{ 'ISOLATED' if vars.env == 'prd' else 'OPEN' }}

# Variables in file paths
<<: !update ./overrides_${vars.env}.yaml
```

Cross-reference a resource's runtime output:
```yaml
group_ids:
- ${resources.group-role-engineers.id}
catalog: ${resources.catalog-dev.name}
```

---

## MCP Server (if configured)

If a Laktory MCP server is configured in `.mcp.json`, prefer the MCP tools over this
static file — they return live, version-accurate documentation from the installed package:

| Tool | Use it to… |
|------|-----------|
| `get_model_docs` | Look up any model's fields before writing YAML |
| `validate_yaml` | Validate a YAML snippet before finalizing |
| `list_models` | Discover all available model names grouped by category |
| `get_version` | Confirm the installed Laktory version |

**`.mcp.json`:**
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
