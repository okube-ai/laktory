
# Variables, Expressions, and References

Laktory uses three distinct mechanisms to make declarations dynamic. They share a curly-brace syntax but differ in purpose, resolution time, and where they are valid.

| Mechanism | Syntax | Resolved | Valid in |
|-----------|--------|----------|----------|
| **Variable** | `${vars.X}` · `${resources.X.id}` | Config / deployment time | Any model field |
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
| 6 *(lowest)* | Laktory settings | `laktory.settings` |

**From model** - any Laktory object can declare its own variables:

```yaml title="cluster.yaml"
name: cluster-${vars.env}
variables:
  env: prd
```

**From environment** - if a variable is not found in declared model variables, Laktory falls back to OS environment variables.

**From settings** - final fallback to `laktory.settings` values.

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
