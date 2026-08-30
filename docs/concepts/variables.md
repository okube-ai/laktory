
# Variables, Expressions, and References

Laktory uses three distinct mechanisms to make declarations dynamic. They share a curly-brace syntax but differ in purpose, resolution time, and where they are valid.

| Mechanism | Syntax | Resolved | Valid in |
|-----------|--------|----------|----------|
| **Variable** | `${vars.X}` · `${resources.X.id}` · `${settings.X}` · `${current_user.X}` | Config / deployment time | Any model field |
| **Expression** | `${{ python expr }}` | Config / deployment time | Any model field |
| **Reference** | `{df}` · `{sources.X}` · `{nodes.X}` | Execution time | Transformer nodes only |

**The `$` rule** - the dollar sign is the tell. Variables and Expressions always start with `$` and are resolved before the pipeline runs. References have no `$` and are resolved at runtime by the transformer engine when it has actual DataFrames in memory.

---

## Variables

A Variable substitutes a named value into any model field. The syntax is `${vars.VARIABLE_NAME}` (or `${var.VARIABLE_NAME}`).

```yaml
name: cluster-${vars.env}
size: ${vars.cluster_size}
variables:
  env: prd
  cluster_size: 2
```

Resolves to `name: cluster-prd` and `size: 2`.

### Declaration sources

When the same variable is declared in multiple places, the following priority applies (highest wins):

| Priority | Source | Example |
|----------|--------|---------|
| 1 *(highest)* | CLI `--var` flags | `--var env=dev` |
| 2 | CLI variable file (`--var-file` or auto-discovered `variables[.<env>].yaml`) | `--var-file my_secrets.yaml` |
| 3 | Stack environment variables (`environments.<env>.variables`) | `stack.yaml` → `environments.dev.variables` |
| 4 | Stack-level variables (`variables`) | `stack.yaml` → `variables` |
| 5 | OS environment variables | `$DATABRICKS_HOST` |
| 6 *(lowest)* | Laktory settings | `laktory.settings` (prefer the explicit `${settings.X}` syntax below over relying on this fallback) |

**From model** - any Laktory object can declare its own variables:

```yaml title="cluster.yaml"
name: cluster-${vars.env}
variables:
  env: prd
```

**From environment** - if a variable is not found in declared model variables, Laktory falls back to OS environment variables.

**From settings** - final fallback to `laktory.settings` values, e.g. `${vars.workspace_root}` resolves to `settings.workspace_root` if no model/env variable of that name exists. This is a legacy alias kept for backward compatibility; prefer the explicit `${settings.X}` syntax described below - it can't be silently shadowed by a same-named variable, and it makes the intent obvious at the call site.

**From CLI** - variables passed at the CLI level override everything:

```bash
laktory deploy --env dev --var profile=MY_PROFILE --var node_type=Standard_DS3_v2
```

A variable file can be provided explicitly or auto-discovered next to the stack file:

```bash
laktory deploy --env dev --var-file variables.yaml
# or with auto-discovery:
laktory deploy --env dev   # loads variables.dev.yaml or variables.yaml if present
```

CLI options are available on all commands: `deploy`, `preview`, `destroy`, `validate`, `build`, and `run`.

### Properties

**Case-insensitive** - variable names are not case-sensitive.

**Inheritance** - models inherit variables from their parent and can override them:

```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ingest
        cluster:
          size: ${vars.cluster_size}
  - name: export-${vars.env}
    variables:
      cluster_size: 1   # override for this job only
variables:
  env: prd
  cluster_size: 2
```

**Nesting** - variables can reference other variables:

```yaml title="stack.yaml"
variables:
  env: prd
  user: laktory
  task_prefix: ${user}-${env}   # resolves to "laktory-prd"
```

### Types

Variables support `int`, `float`, `string`, `boolean`, and complex objects (lists and dicts):

```yaml title="stack.yaml"
variables:
  env: dev
  job_tags:
    - laktory
    - poc
  default_cluster:
    name: default-cluster
    size: 2
```

For advanced substitutions, regex patterns are also supported:

```yaml title="stack.yaml"
cluster:
  - name: ${custom_prefix.catalog.schema}
variables:
  r"\$\{custom_prefix\.(.*?)\}": r"${\1}"
```

### Resource variables

The `resources.*` namespace exposes deployed resource output attributes as variables, automatically populated by Laktory from the Terraform backend:

```yaml title="stack.yaml"
tasks:
  - task_key: pipeline
    pipeline_task:
      pipeline_id: ${resources.my-pipeline.id}
```

Here `${resources.my-pipeline.id}` resolves to the ID of the deployed `my-pipeline` resource. The resource must be part of the current stack.

### Settings

The `settings.*` namespace exposes `laktory.settings` values (`workspace_root`, `build_root`, `runtime_root`, `dataframe_backend`, `dataframe_api`) as variables, so a value configured once under `stack.yaml`'s `settings:` block can be reused anywhere else in the stack without duplicating the literal:

```yaml title="stack.yaml"
settings:
  workspace_root: /Users/${vars.username}/.laktory/${vars.env}/
resources:
  databricks_workspacetrees:
    app:
      source: ./app/
      path: ${settings.workspace_root}app   # same root, reused explicitly
variables:
  username: jane.doe@example.com
  env: dev
```

`settings:` itself accepts `${vars.X}` (as shown above) - resolved the same way as any other field, once `inject_vars()` runs (i.e. as part of `build`, `preview`, `deploy`, `destroy`, or `validate`). Right after a `Stack` is constructed, before any of those commands run, `settings.*` fields are still unresolved templates, same as any other `${vars.X}`-templated field (e.g. `Stack.name`).

`${resources.X.y}` cannot be used inside `settings:` - it is resolved by Terraform at `plan`/`apply` time, after `settings:` has already been applied, so it would never become a real value. Laktory raises a validation error if you try.

A settings field cannot reference a sibling settings field in the same block (e.g. `build_root: ${settings.workspace_root}x` inside the same `settings:` you're defining `workspace_root` in) - it would see the *previous* value, not the one being defined alongside it. Reference another settings value from *outside* the `settings:` block instead, as in the example above.

For what `settings.workspace_root` actually controls, its default, and how to auto-scope it (and Terraform state) to your own user/stack/environment with almost no configuration, see [Workspace Root](workspaceroot.md).

### Current User

The `current_user.*` namespace exposes your live Databricks identity - currently just `user_name` - resolved via the Databricks SDK against the `DatabricksProvider` in your stack:

```yaml title="stack.yaml"
resources:
  providers:
    databricks: {}
  databricks_workspacetrees:
    app:
      source: ./app/
      path: /Users/${current_user.user_name}/app
```

Unlike `settings.*`, `current_user.*` isn't backed by a stack config field - it's resolved lazily, only when `${current_user.X}` is actually referenced somewhere in the stack, via one live SDK call (`workspace_client.current_user.me()`). A stack that never references it makes no network call for it. Referencing it without a `DatabricksProvider` in the stack raises a clear error rather than silently leaving the template unresolved.

If the stack also uses `settings.workspace_root: "user_root"` (see [Workspace Root](workspaceroot.md)) and/or `terraform.backend.databricks_workspace: true`, all three share the same single SDK lookup - no redundant calls.

---

## Expressions

An Expression evaluates an inline Python statement and injects the result into a field. The syntax is `${{ PYTHON_EXPRESSION }}`.

```yaml title="stack.yaml"
cluster:
  - name: pipeline-${vars.env}
    size: ${{ 4 if vars.env == 'prd' else 2 }}
variables:
  env: prd
```

`size` evaluates to `4`. Any valid Python expression is supported, including dict lookups:

```yaml
size: ${{ vars.sizes[vars.env] }}
variables:
  env: prd
  sizes:
    dev: 2
    prd: 4
```

`settings.X` and `current_user.X` are also available inside expressions, alongside `vars.X`:

```yaml
name: ${{ 'prod-' + settings.workspace_root if vars.env == 'prd' else 'dev' }}
path: ${{ '/Users/' + current_user.user_name + '/app' }}
```

### Context objects

Certain Python objects are available inside expressions depending on context.

**`pipeline`** - available inside a pipeline and all its children:

```yaml title="pipeline.yaml"
orchestrator:
  type: DATABRICKS_JOB
  name: job-${{ pipeline.name }}
```

**`pipeline_node`** - available inside a pipeline node and all its children:

```yaml title="pipeline.yaml"
nodes:
  - name: slv_prices
    primary_keys:
      - tstamp
      - symbol
    sinks:
    - merge_cdc_options:
        primary_keys: ${{ pipeline_node.primary_keys }}
```

---

## References

A Reference identifies a DataFrame inside a [transformer](transformer.md) expression or method argument. References use plain `{...}` with **no `$`** and are resolved at execution time when the DataFrames are live in memory. They are not variables and cannot appear in arbitrary model fields.

Three references are available:

| Reference | Points to |
|-----------|-----------|
| `{df}` | The flowing DataFrame - the primary source on the first transformer step, the output of the previous step on subsequent steps |
| `{sources.name}` | A named source declared on the pipeline node |
| `{nodes.X}` | The output DataFrame of upstream pipeline node `X` |

```yaml
nodes:
- name: slv_stocks
  sources:
  - name: prices
    node_name: brz_stock_prices
  - name: metadata
    node_name: brz_stock_metadata
  transformer:
    nodes:
    # {sources.X} in a SQL expression
    - expr: |
        SELECT p.symbol, p.open, m.currency
        FROM {sources.prices} p
        LEFT JOIN {sources.metadata} m ON p.symbol = m.symbol

    # {sources.X} in a method argument
    - func_name: join
      func_kwargs:
        other: "{sources.metadata}"
        on: symbol

    # {df} refers to the output of the previous step
    - expr: SELECT * FROM {df} WHERE open > 100

    # {nodes.X} reaches any upstream node by name
    - expr: SELECT * FROM {df} UNION ALL SELECT * FROM {nodes.brz_stock_prices}
```

See [Transformer - DataFrame References](transformer.md#dataframe-references) for the full reference.

---

## Variable Injection

??? "API Documentation"
    [`laktory.models.BaseModel.inject_vars`][laktory.models.BaseModel]<br>

Variables and Expressions are injected during deployment, typically after serialization (`model_dump`). You can manually trigger injection using `model.inject_vars()`.

---

## Rendered file content

Variable injection normally applies to model *field values* only. A handful of resources instead point at a **local file whose content is uploaded verbatim** - `Dashboard.file_path`, `WorkspaceFile.source`, `Notebook.source`, `DbfsFile.source`, and the many files generated by `WorkspaceTree`. By default, the content of these files is **not** variable-resolved - they are uploaded byte-for-byte, exactly as they are on disk.

You can opt a resource into rendering its file content:

**Single-file resources** - set `render_vars: true`:

```yaml title="dashboard.yaml"
display_name: databricks-costs
file_path: ./dashboards/databricks_costs.json  # contains ${vars.catalog}
warehouse_id: a7d9f2kl8mp3q6rt
render_vars: true
variables:
  catalog: ${vars.env}_catalog
```

**`WorkspaceTree`** (many files at once) - opt in per glob pattern (relative to `source`); a bare pattern like `*.json` matches any file with that extension at any depth, while a scoped pattern like `configs/*.yaml` only matches within that subdirectory:

```yaml title="app_tree.yaml"
source: ./app/
path: /apps/myapp
render_paths:
  - '*.json'
  - configs/*.yaml
```

Resolved content is staged under `settings.build_root` (a copy - the original file on disk is never modified), and the resource's `source`/`file_path` is repointed at the staged copy so the Databricks Terraform provider still does its own upload/diffing at apply time. Rendering happens as part of `Stack.build()`, which `laktory build`, `preview`, `deploy`, and `destroy` all trigger automatically - so the staged content always reflects the target environment's variables.

**Complex variables** (dict/list) work here too, unlike in a plain YAML field. A `${vars.x}` resolving to a dict or list is JSON-serialized into the surrounding text rather than replacing it wholesale, so it stays valid in JSON and YAML content:

```yaml title="dashboard.yaml"
variables:
  tags: {bu: finance, env: dev}
```
```json title="databricks_costs.json (before)"
{"tags": ${vars.tags}}
```
```json title="staged copy (after)"
{"tags": {"bu": "finance", "env": "dev"}}
```
