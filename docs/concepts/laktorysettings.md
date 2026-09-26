`settings` on a [`Stack`](stack.md) (or per-environment, under `environments.{env}.settings`) configures Laktory-wide behavior via [`LaktorySettings`][laktory.models.stacks.stack.LaktorySettings]:

| Setting | Default | Controls |
|---|---|---|
| [`workspace_root`](#workspace-root) | `/.laktory/` | Databricks Workspace root where deployed objects (notebooks, workspace files, dashboards, ...) land |
| [`runtime_root`](#runtime-root) | `/laktory/` | Root Laktory writes pipeline runtime artifacts (checkpoints) to |
| [`build_root`](#build-root) | Laktory cache directory | Local directory for generated build artifacts |
| [`dataframe_backend`](#dataframe-backend-and-api) | `PYSPARK` | DataFrame backend (`POLARS` / `PYSPARK`) |
| [`dataframe_api`](#dataframe-backend-and-api) | `NARWHALS` | DataFrame API (`NARWHALS` / `NATIVE`) |
| [`reset_mode`](#reset-mode) | `DROP` | How sinks are reset on a full refresh or reset run (`DROP` / `TRUNCATE`), for sinks, nodes and pipelines that don't set one |

Defaults come from the matching `LAKTORY_*` environment variables (e.g. `LAKTORY_RESET_MODE`). A
value set in the Stack `settings` overrides them. For `dataframe_backend`, `dataframe_api` and
`reset_mode`, values set on a pipeline, pipeline node or sink always take precedence.

Settings values can reference [variables](variables.md) via `${vars.x}`, and are themselves reusable elsewhere in the stack via `${settings.x}` (see [Variables - Settings](variables.md#settings)):

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
```

## Workspace Root

`settings.workspace_root` is the root directory in your Databricks workspace where deployed objects land - notebooks, workspace files, dashboards, alerts, queries, and more. It defaults to a flat, fixed path:

```
/.laktory/
```

This doesn't scope by who deployed it, which stack, or which environment - two stacks (or two environments of the same stack) deployed to the same workspace write under that same root by default, and collide if their relative paths overlap (e.g. both have a `notebooks/ingest.py`). **A future major version will change the default to `user_root`** (below) to avoid this out of the box.

Set `workspace_root` to the reserved value `user_root` to opt into that scoped root today:

```yaml title="stack.yaml"
settings:
  workspace_root: user_root
```

This resolves to `/Users/{you}/.laktory/{stack_name}/{env_name}/` and requires a `DatabricksProvider` in the stack, since it resolves your username via a live SDK call.

Whatever `workspace_root` is set to, it's the default deployment location for objects like `Notebook`, `WorkspaceFile`, `WorkspaceTree`, `Dashboard`, `Alert`, and `Query` - most (e.g. `Notebook`, `WorkspaceFile`) can escape it via their own `path`, but `Dashboard`/`Alert`/`Query` always deploy under it.

Separately, `terraform.backend.databricks_workspace: true` stores Terraform state as a file under your own Databricks user directory instead of requiring external storage - resolved the same way as `user_root`, and nested inside the same root when both are used together.

## Runtime Root

`settings.runtime_root` is the root Laktory writes pipeline *runtime* artifacts to - today, that's exclusively Spark structured-streaming checkpoints (sink checkpoints and pipeline node expectations checkpoints). It defaults to:

```
/laktory/
```

an absolute path that historically resolves through the DBFS FUSE mount on Databricks compute. Newer workspaces - serverless compute, or workspaces created after DBFS legacy features were disabled - don't have that mount, so this default may not work there. This default is likely to change in a future version; no replacement has been decided yet.

On a workspace without DBFS, set `runtime_root` explicitly to a path under a [Unity Catalog Volume](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html) - the best alternative, since Volumes are exposed via an automatic FUSE mount on all Databricks compute (classic and serverless), so checkpoint read/write/purge all work through the plain filesystem with nothing else to configure:

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

## Reset Mode

`reset_mode` (`DROP` / `TRUNCATE`, default `DROP`) is how a data sink is reset on a full refresh or
a reset run. The value used for a sink is the first one set among: the sink, its
[`PipelineNode`](pipeline.md), its [`Pipeline`](pipeline.md), and this setting:

```yaml title="stack.yaml"
settings:
  reset_mode: TRUNCATE
```

It can also be set via the `LAKTORY_RESET_MODE` environment variable. `DELETE_WHERE` cannot be
set here (or on a `Pipeline`/`PipelineNode`) - it's only valid set directly on a sink, since its
deletion predicate is inherently sink-specific; doing so anywhere else raises a validation error.
See
[Data Sources and Sinks - Reset Modes](sourcessinks.md#reset-modes) for the full explanation and
the sink-level `reset_delete_where` field it pairs with.
