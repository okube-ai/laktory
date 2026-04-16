The `workflows-dab` template generates sample Laktory pipeline files for use with Databricks
[Declarative Automation Bundles (DAB)](../concepts/dab.md). In this case, the deployment is managed by DAB
and Laktory can be used to define pipelines and automatically create the associated Job and Pipeline resources.

### Prerequisites

- An existing `databricks.yml` bundle configuration
- The Databricks CLI installed and authenticated (`databricks auth login`)
- A Python virtual environment with Laktory installed

### Create Files
Run the following command in the root of your bundle:

```cmd
laktory quickstart -t workflows-dab
```

Laktory will copy the sample pipeline files and print the changes required in your `databricks.yml`.

#### Files
After running the quickstart command, the following structure is created:

```terminal
.
├── laktory
│   └── pipelines
│       ├── pl-taxi-trips.yml
│       └── pl-tpch.yml
└── resources
    └── tpch.job.yml
```

#### Pipelines Directory
The `laktory/pipelines/` directory contains two example Laktory pipeline definitions:

- **`pl-taxi-trips.yml`** — A Databricks Job pipeline ingesting from `samples.nyctaxi.trips`.
  Demonstrates DataFrame API transformations (derived columns, custom aggregations) and SQL-based
  view creation.
- **`pl-tpch.yml`** — A Lakeflow Declarative Pipeline ingesting from `samples.tpch`. Demonstrates
  column selection and renaming using the DataFrame API, SQL aggregation.

Both pipelines source data from the built-in `samples` Unity Catalog, available in every
Databricks workspace — no data upload required.

#### Resources Directory
The `resources/tpch.job.yml` file defines a raw DAB Databricks Job that triggers the `pl-tpch`
Declarative Pipeline on a daily schedule. It references the pipeline using DAB's
`${resources.pipelines.pl-tpch.id}` substitution syntax and is included via the
`include: - resources/*.yml` directive in your `databricks.yml`.

### Update databricks.yml

Add the following sections to your `databricks.yml`:

```yaml
variables:
  catalog:
    default: <your_catalog>
  dab_workspace_root:
    default: ${workspace.root_path}
  laktory_pipelines_dir:
    default: ./laktory/pipelines

sync:
  paths:
    - ./laktory
  include:
    - ./laktory/.build/**

include:
  - resources/*.yml

python:
  venv_path: .venv
  resources:
    - 'laktory.dab:build_resources'
```

The `laktory.dab:build_resources` hook reads your pipeline YAML files at bundle deployment time,
generates the corresponding Databricks resources (Jobs, Declarative Pipelines, workspace files),
and injects them into the bundle — no manual resource authoring required.

### Deploy

Deploy your bundle to the `dev` target:

```cmd
databricks bundle deploy --target dev
```

Laktory will build all pipeline resources and merge them with any resources already declared in
your bundle. After deployment, both `pl-taxi-trips` and `pl-tpch` will appear in your Databricks
workspace alongside the `job-tpch` job that schedules the Declarative Pipeline.

### Next Steps

- Customize the pipeline YAML files with your own sources, sinks, and transformations.
- Add your own variables in `databricks.yml` and reference them in pipeline files via `${var.<name>}`.
- See [DAB](../concepts/dab.md) for the full DAB integration reference.
