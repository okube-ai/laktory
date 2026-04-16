??? "API Documentation"
    [`laktory.dab.build_resources`][laktory.dab.build_resources]<br>

[Declarative Automation Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html) is Databricks' native 
infrastructure-as-code solution. Laktory integrates with it through a Python resource hook that discovers pipeline YAML 
files, generates the required configuration files, and returns Job and DLT Pipeline resources, bypassing Laktory's own 
deployment mechanism.

If your team already uses the Databricks CLI and `databricks.yml` to manage workspace 
resources, DABs integration get your started quickly.


## How It Works

When `databricks bundle deploy` is executed, the Databricks CLI calls the registered Python hook 
`laktory.dab:build_resources`. Laktory then:

1. Scans the configured pipeline directory for `*.yaml` / `*.yml` files
2. Loads each pipeline and injects bundle variables
3. Writes a JSON config file per pipeline to `laktory/.build/pipelines/` for the pipeline executor to read at runtime
4. Returns a DABs `Resources` object containing one Job or DLT Pipeline resource per pipeline

DABs then syncs the `laktory/.build/` directory (including the generated config files and the DLT notebook) to the 
Databricks workspace, and deploys the Job/Pipeline resources.


## Setup `databricks.yml`

Add the following to your bundle configuration:

```yaml title="databricks.yml"
variables:
  dab_workspace_root:        # required: workspace path where Laktory files will be synced
    default: ${workspace.root_path}
  laktory_pipelines_dir:     # local directory containing pipeline YAML files
    default: ./laktory/pipelines

sync:
  paths:
    - ./laktory            # sync pipeline YAMLs and the generated .build/ directory
  include:
    - ./laktory/.build/**  # force-include even if laktory/.build/ is in .gitignore

python:
  venv_path: .venv
  resources:
    - 'laktory.dab:build_resources'
```

The `./laktory/.build/` directory contains generated files (pipeline config JSON, DLT notebook) and is typically added to 
`.gitignore`. The `sync.include` directive tells DABs to sync it to the workspace regardless.

The `dab_workspace_root` variable must be set to `${workspace.root_path}` so Laktory can compute the correct 
workspace path for the config files.


## Pipeline Discovery

Laktory scans `{laktory_pipelines_dir}` for `*.yaml` and `*.yml` files. Each file is expected to contain a single 
[pipeline](pipeline.md) definition. Pipelines without an orchestrator are skipped silently.

Multiple directories are supported via a comma-separated list:

```yaml title="databricks.yml"
variables:
  laktory_pipelines_dir:
    default: ./laktory/pipelines/
```


## Orchestrators

Each pipeline declares its orchestrator type, which determines what DABs resource is created.

### Databricks Pipeline (DLT)

Setting `type: DATABRICKS_PIPELINE` generates a `databricks.bundles.pipelines.Pipeline` resource. All Laktory DLT 
pipelines share a single entry-point notebook (`dlt_laktory_pl.py`) that is automatically copied to the build directory 
and synced to the workspace.

```yaml title="laktory/pipelines/pl-stock-prices.yml"
name: pl-stock-prices

orchestrator:
  type: DATABRICKS_PIPELINE
  serverless: true
  catalog: ${var.catalog}
  schema: market

nodes:
  - name: brz_stock_prices
    source:
      path: /Volumes/${var.catalog}/sources/landing/stock_prices/
      format: JSON
    sinks:
      - table_name: brz_stock_prices

  - name: slv_stock_prices
    source:
      node_name: brz_stock_prices
    sinks:
      - table_name: slv_stock_prices
    transformer:
      nodes:
        - expr: SELECT symbol, close, created_at FROM {df}
          func_type: SQL
```

Limitations of DLT orchestrators:

- Pipeline nodes with [views](sourcessinks.md) are not supported

### Databricks Job

Setting `type: DATABRICKS_JOB` generates a `databricks.bundles.jobs.Job` resource. Each pipeline node becomes a 
separate job task, and task dependencies mirror the pipeline DAG.

```yaml title="laktory/pipelines/pl-taxi-trips.yml"
name: pl-taxi-trips

orchestrator:
  type: DATABRICKS_JOB
  serverless_environment_version: "2"
  name: pl-taxi-trips

nodes:
  - name: brz_taxi_trips
    source:
      table_name: samples.nyctaxi.trips
    sinks:
      - catalog_name: ${var.catalog}
        schema_name: taxis
        table_name: brz_taxi_trips

  - name: slv_taxi_trips
    source:
      node_name: brz_taxi_trips
    sinks:
      - catalog_name: ${var.catalog}
        schema_name: taxis
        table_name: slv_taxi_trips
    transformer:
      nodes:
        - expr: SELECT *, trip_distance * 1.60934 AS trip_distance_km FROM {df}
          func_type: SQL
```

The Job orchestrator supports views and both serverless and classic job clusters. Set 
`serverless_environment_version` for serverless execution, or define `job_clusters` for classic compute.


## Variable Injection

Bundle variables declared in `databricks.yml` are automatically injected into pipeline models at load time. Both 
DABs-style (`${var.name}`) and Laktory-style (`${vars.name}`) syntax are supported. Pipeline-level 
[variables](variables.md) take precedence over bundle variables when names conflict.

```yaml title="databricks.yml"
variables:
  catalog:
    description: The Unity Catalog to deploy into

targets:
  dev:
    variables:
      catalog: dev
  prod:
    variables:
      catalog: prod
```

```yaml title="laktory/pipelines/pl-stock-prices.yml"
name: pl-stock-prices

orchestrator:
  type: DATABRICKS_PIPELINE
  catalog: ${var.catalog}   # resolved from bundle variable at deploy time
  schema: market
```


## Settings

Two settings control where Laktory writes and reads files during bundle resolution:

| Setting          | Environment variable     | Description                                                          |
|------------------|--------------------------|----------------------------------------------------------------------|
| `build_root`     | `LAKTORY_BUILD_ROOT`     | Local directory for generated config JSON files and the DLT notebook |
| `workspace_root` | `LAKTORY_WORKSPACE_ROOT` | Workspace path where Laktory files are synced by DABs              |

Both are auto-configured from the bundle context when left at their defaults. The `build_root` is set to 
`{bundle_root}/laktory/.build/` and `workspace_root` is derived as 
`{dab_workspace_root}/files/laktory/.build/`. Explicit overrides via environment variables take priority.
