
# Variables and Expressions

When declaring models in Laktory, it's not always practical, desirable, or even possible to hardcode certain properties. For example, the catalog name in a pipeline declaration might depend on the deployment environment. In many cases, you'll also want to share properties across multiple objects. Laktory introduces **model variables** to solve this problem.

## Syntax

To use a variable, follow this syntax: `${vars.VARIABLE_NAME}` or `${var.VARIABLE_NAME}`.

## Declaration

When the same variable is declared in multiple places, the following priority applies (highest wins):

| Priority      | Source                                                                       | Example                                     |
|---------------|------------------------------------------------------------------------------|---------------------------------------------|
| 1 *(highest)* | CLI `--var` flags                                                            | `--var env=dev`                             |
| 2             | CLI variable file (`--var-file` or auto-discovered `variables[.<env>].yaml`) | `--var-file my_secrets.yaml`                |
| 3             | Stack environment variables (`environments.<env>.variables`)                 | `stack.yaml` → `environments.dev.variables` |
| 4             | Stack-level variables (`variables`)                                          | `stack.yaml` → `variables`                  |
| 5             | OS environment variables                                                     | `$DATABRICKS_HOST`                          |
| 6 *(lowest)*  | Laktory settings                                                             | `laktory.settings`                          |

### From Model
Any object declared with Laktory can receive variables as part of its data model:

```yaml title="cluster.yaml"
name: cluster-${vars.env}
size: ${vars.cluster_size}
variables:
  env: prd
  cluster_size: 2
```

When resolved:

- `name` becomes `cluster-prd`.
- `size` becomes `2`.

### From Environment
When resolving a variable, Laktory first searches for declared model variables. If not found, it falls back to environment variables.

### From Settings
If a variable has not been resolved from model variables nor environment variables, it looks up `laktory.settings` values.

### From CLI

Variables can be injected at the CLI level, overriding anything declared in the stack YAML or variable files.

**Inline flags** (`--var`, repeatable):
```bash
laktory deploy --env dev --var profile=MY_PROFILE --var node_type=Standard_DS3_v2
```

**Variable file** (`--var-file`):
```bash
laktory deploy --env dev --var-file variables.yaml
```

The file must be YAML and supports all variable types — including complex objects:

```yaml title="variables.yaml"
profile: MY_PROFILE
node_type: Standard_DS3_v2
default_cluster:
  num_workers: 4
  node_type_id: Standard_DS3_v2
```

**Auto-discovery** — when `--var-file` is not provided, Laktory automatically loads a variable file if one is present next to the stack file, in this order:

1. `variables.<env>.yaml` — env-specific (e.g. `variables.dev.yaml`)
2. `variables.yaml` — base fallback

This makes per-environment secrets easy to manage: commit a `variables.yaml` template, add `variables.*.yaml` to `.gitignore`, and deploy with just:

```bash
laktory deploy --env dev
```

`--var` flags always take precedence over the variable file.

These CLI options are available on all commands: `deploy`, `preview`, `destroy`, `validate`, `build`, and `run`.

## Properties

### Case Sensitivity
All variables are **case-insensitive**.

### Inheritance
Models inherit variables from their parent. They can also declare new variables or override parent variables.

```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ingest
        cluster:
          size: ${vars.cluster_size}
      - name: process
        cluster:
          size: ${vars.cluster_size}
  - name: export-${vars.env}
    tasks:
      - name: export
        cluster:
          size: ${vars.cluster_size}
    variables:
      cluster_size: 1
variables:
  env: prd
  cluster_size: 2
```

In this example:

- `pipeline-prd` tasks use clusters of size `2`.
- `export-prd` tasks use clusters of size `1` due to the local override of `cluster_size`.

### Nesting
Variables can reference other variables:

```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ${task_prefix}-ingest
      - name: ${task_prefix}-process
    variables:
      task_prefix: ${user}-${env}
providers:
  databricks:
    host: ${vars.databricks_host}
variables:
  env: prd
  user: laktory
  databricks_host: ${vars.DATABRICKS_HOST_DEV}
```

Results:

- Task names: `laktory-prd-ingest` and `laktory-prd-process`.
- `databricks_host` resolves to the environment variable `DATABRICKS_HOST_DEV`.

## Types

### Simple
Supports `int`, `float`, `string`, and `boolean`.

### Complex
Supports complex objects like lists and dictionaries:

```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ingest
        cluster: ${vars.default_cluster}
      - name: ${task_prefix}-process
        cluster: ${vars.default_cluster}
    tags: ${vars.job_tags}
variables:
  env: dev
  job_tags:
    - laktory
    - poc
  default_cluster:
    name: default-cluster
    size: 2
```

Here:

- `job_tags` is a list of tags.
- `default_cluster` defines reusable cluster configurations.

### Regex
For advanced substitutions, use regex patterns:

```yaml title="stack.yaml"
cluster:
  - name: ${custom_prefix.catalog.schema}
variables:
  r"\$\{custom_prefix\.(.*?)\}": r"${\1}"
```

Resolving the cluster name yields `catalog.schema`.

## Expressions

Use `${{ PYTHON_EXPRESSION }}` for dynamic attribute values:

```yaml title="stack.yaml"
cluster:
  - name: pipeline-${vars.env}
    size: ${{ 4 if vars.env == 'prd' else 2 }}
variables:
  env: prd
```

Here, `size` evaluates to `4`. Any valid inline Python expression is supported.

You can also use variables as dictionary keys:

```yaml title="stack.yaml"
cluster:
  - name: pipeline-${vars.env}
    size: ${{ vars.sizes[vars.env] }}
variables:
  env: prd
  sizes:
    dev: 2
    prd: 4
```

### Expressions Objects
Under certain context, specific python objects are available when evaluating expressions

#### Pipeline
When inside a pipeline and its child (node, source, sink, transformer, orchestrator, etc) the pipeline object is available.

```yaml title="pipeline.yaml"
pipeline:
  name: pl-stocks-prices
  orchestrator:
    type: DATABRICKS_JOB
    name: job-${{ pipeline.name }}
```


#### Pipeline Node
When inside a pipeline node and its child (source, sink, transformer, orchestrator, etc) the pipeline_node object is available.

```yaml title="pipeline.yaml"
pipeline:
  name: pl-stocks-prices
  nodes:
    - name: slv_prices
      primary_keys:
        - tstamp
        - symbol
      sink:
        merge_cdc_options:
          primary_keys: ${{ pipeline_node.primary_keys }}
```

## Special Cases

### Pipeline Nodes
When defining SQL transformations, Laktory allows referencing:

- The previous node’s DataFrame using `{df}`.
- Specific nodes using `{nodes.node_name}`.

Example:

```sql
SELECT 
  * 
FROM 
  {df} 
UNION 
  SELECT * FROM {nodes.node_01} 
UNION 
  SELECT * FROM {nodes.node_02}
```

### Resources
Variables can reference deployed resource outputs:

```yaml title="stack.yaml"
name: my-job-${vars.env}
tasks:
  - task_key: pipeline
    pipeline_task:
      pipeline_id: ${resources.my-pipeline.id}
```

Here:

- `pipeline_id` dynamically references `my-pipeline`'s ID.

**Note:** Resource variables are automatically populated by Laktory based on the Terraform backend. The resource must be deployed as part of the current stack.

## Variable Injection

??? "API Documentation"
    [`laktory.models.BaseModel.inject_vars`][laktory.models.BaseModel]<br>

Variables are injected during deployment, typically after serialization (`model_dump`). However, you can manually trigger injection using `job.inject_vars()`.
