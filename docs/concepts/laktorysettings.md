`settings` on a [`Stack`](stack.md) (or per-environment, under `environments.{env}.settings`) configures Laktory-wide behavior via [`LaktorySettings`][laktory.models.stacks.stack.LaktorySettings]:

| Setting | Default | Controls |
|---|---|---|
| [`workspace_root`](#workspace-root) | `/.laktory/{stack_name}/{env_name}/` | Databricks Workspace root where deployed objects (notebooks, workspace files, dashboards, ...) land |
| [`runtime_root`](#runtime-root) | `/.laktory/` (Databricks), `./.laktory/` (local) | Root Laktory writes pipeline runtime artifacts (checkpoints) to |
| [`build_root`](#build-root) | Laktory cache directory | Local directory for generated build artifacts |
| [`dataframe_backend`](#dataframe-backend-and-api) | `PYSPARK` | DataFrame backend (`POLARS` / `PYSPARK`) |
| [`dataframe_api`](#dataframe-backend-and-api) | `NARWHALS` | DataFrame API (`NARWHALS` / `NATIVE`) |

A value set in the Stack `settings` overrides internal laktory settings. For `dataframe_backend` and
`dataframe_api`, values set on a pipeline or pipeline node always take precedence.

Settings values can reference [variables](variables.md) via `${vars.x}`, and are themselves reusable elsewhere in the stack via `${settings.x}` (see [Variables - Settings](variables.md#settings)):

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
```

## Workspace Root

`settings.workspace_root` is the root directory in your Databricks workspace where deployed objects land - notebooks, workspace files, dashboards, alerts, queries, and more. Inside a stack, it defaults to a root scoped by stack and environment:

```
/.laktory/{stack_name}/{env_name}/
```

(`/.laktory/{stack_name}/` when deploying without environment), so two stacks, or two environments of the same stack, deployed to the same workspace never collide. The path doesn't depend on who deploys: a developer and a CI service principal deploy to the same location. Outside a stack (e.g. a pipeline executed on its own), the default is `/.laktory/`. The `LAKTORY_WORKSPACE_ROOT` environment variable, when set, takes precedence over the scoped default.

Set `workspace_root` to the reserved value `user_root` to deploy under your own user directory instead:

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
```

This resolves to `/Users/{you}/.laktory/{stack_name}/{env_name}/` and requires a `DatabricksProvider` in the stack, since it resolves your username via a live SDK call. It's typically enabled for a development environment only, with [environment settings](#environment-settings):

```yaml title="stack.yaml"
environments:
  dev:
    settings:
      workspace_root: user_root   # /Users/{you}/.laktory/{stack_name}/dev/
  prod: {}                        # /.laktory/{stack_name}/prod/
```

`user_root` can also be selected through a variable, e.g. `workspace_root: ${vars.workspace_root}`.

!!! note "Upgrading from 0.13"
    Before 0.14, the default was `/.laktory/` for every stack and environment. Set `workspace_root: /.laktory/` to keep deploying there.

Whatever `workspace_root` is set to, it's the default deployment location for objects like `Notebook`, `WorkspaceFile`, `WorkspaceTree`, `Dashboard`, `Alert`, and `Query` - most (e.g. `Notebook`, `WorkspaceFile`) can escape it via their own `path`, but `Dashboard`/`Alert`/`Query` always deploy under it.

Separately, `terraform.backend.databricks_workspace: true` stores Terraform state as a file under your own Databricks user directory instead of requiring external storage - resolved the same way as `user_root`, and nested inside the same root when both are used together.

## Environment Settings

Settings declared under `environments.{env}.settings` are merged field by field over the stack `settings` (like `variables`), and only apply when deploying that environment:

```yaml title="stack.yaml"
settings:
  runtime_root: /Volumes/main/laktory/runtime/
environments:
  dev:
    settings:
      workspace_root: user_root   # runtime_root inherited from the stack settings
  prod: {}
```

## Runtime Root

`settings.runtime_root` is the root Laktory writes pipeline *runtime* artifacts to - today, that's exclusively Spark structured-streaming checkpoints (sink checkpoints and pipeline node expectations checkpoints). Unless set explicitly, it defaults to:

- `/.laktory/` for pipelines with a `LAKEFLOW_JOB` or `LAKEFLOW_DECLARATIVE_PIPELINE` orchestrator: an absolute path resolved on DBFS on Databricks compute
- `./.laktory/` otherwise (local execution, including the `SPARK_DECLARATIVE_PIPELINE` orchestrator), relative to the current working directory

Pipeline checkpoints are stored under `{runtime_root}/pipelines/{pipeline_name}/`.

New Databricks workspaces don't support DBFS, so the `/.laktory/` default doesn't work there. On a workspace without DBFS, set `runtime_root` explicitly to a path under a [Unity Catalog Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html) - the best alternative, since Volumes are exposed via an automatic FUSE mount on all Databricks compute (classic and serverless), so checkpoint read/write/purge all work through the plain filesystem with nothing else to configure:

```yaml title="stack.yaml"
settings:
  runtime_root: /Volumes/{catalog_name}/{schema_name}/{volume_name}/{some_path}/
```

Workspace Files (`/Workspace/...`) is not an alternative here - Databricks does not support it as a Spark structured-streaming checkpoint location, only Volumes does.

## Build Root

`settings.build_root` is the local directory Laktory writes generated build artifacts to - pipeline config JSON, resource files, and (for orchestrators like Lakeflow Declarative Pipeline) the generated notebook. Left at its default (an empty string), it resolves to the Laktory cache directory.

Override it when file generation is delegated to a third party that expects those files at a specific path - most notably [Databricks Asset Bundles](dab.md#settings), which auto-configures `build_root` to `{bundle_root}/laktory/.build/` from the bundle context unless overridden explicitly (via `settings.build_root` or the `LAKTORY_BUILD_ROOT` environment variable - see [DAB - Settings](dab.md#settings) for the full auto-configuration behavior, including its companion `workspace_root` override).

## DataFrame Backend and API

`dataframe_backend` (`POLARS` / `PYSPARK`) and `dataframe_api` (`NARWHALS` / `NATIVE`) set stack-wide defaults for how pipelines process data, overridable per [`Pipeline`](pipeline.md) or `PipelineNode`. Unlike the three root settings above, they aren't path/location configuration, so their full explanation lives with the rest of the DataFrame documentation: see [Data Pipeline](pipeline.md) for backend selection and [Data Transformer](transformer.md) for the NARWHALS/NATIVE API choice.
