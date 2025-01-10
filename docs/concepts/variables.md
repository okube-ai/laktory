When declaring models in Laktory, it's not always practical, desirable, or even possible to hardcode certain properties. 
For instance, in a pipeline declaration, the catalog name might depend on the environment where the pipeline is deployed. 
In many cases, you'll also want to share properties amongst multiple objects. Laktory introduces model variables to 
solve this problem.

## Syntax
The syntax to use a variable is `${vars.VARIABLE_NAME}`.

## Declaration

### From Model
Any object declared with Laktory can receive variables as part of their data model. 

```yaml title="cluster.yaml"
name: cluster-${vars.env}
size: ${vars.cluster_size}
variables:
    env: prd
    cluster_size: 2
```
When resolved, cluster `name` will be `cluster-prd` and cluster `size` will be 2.

### From Environment
When resolving a variable, it will search for declared model variables first, but will
fallback on environment variables if not found.

### From CLI
Injecting variables from the CLI is not currently support, but will be in the future.

## Properties

### Case Sensitivity
All variables are case-insensitive.

### Inheritance
Any model inherits variables from its parent. It can also declare a new variable
or update a parent one.
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
In this case job `pipeline-prd` will have its tasks use clusters of size 2, while
tasks of job `export-prd` will have its tasks use clusters of size 1 because of the
local overwrite of `cluster_size` variable.

### Nesting
Variables can be nested or, in other words, declared from another variable.
```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ${task-prefix}-ingest
      - name: ${task-prefix}-process
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
In this example, the tasks names will be `laktory-prd-ingest` and `laktory-prd-process`.
The `databricks_host` is declared to be used as an alias for an assumed to be existing
environment variable named `DATABRICKS_HOST_DEV`.

## Types
### Simple
Simple objects such as int, float, string and booleans are all supported.

### Complex
Variable can even be declared as complex objects such as lists and dictionaries.

```yaml title="stack.yaml"
jobs:
  - name: pipeline-${vars.env}
    tasks:
      - name: ingest
        cluster: ${vars.default_cluster}
      - name: ${task-prefix}-process
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
In this example, the variable `job_tags` defines a list of tags used in jobs and 
`default_cluster` defines comprehensively the cluster to be re-used for each task.

### Regex
For advanced substitutions, regex expressions may also be used. In this case, the variable
name must be the pattern to search for and the value must be the replacement string.

```yaml title="stack.yaml"
cluster:
  - name: ${custom_prefix.catalog.schema}
variables:
  r"\$\{custom_prefix\.(.*?)\}": r"${\1}"
```
Resolving for the cluster name would result in `catalog.schema`.


## Expressions
A special syntax `${{ PYTHON_EXPRESSION }}` can also be used to dynamically change
attribute values based on the values of the variables.

```yaml title="stack.yaml"
cluster:
  - name: pipeline-${vars.env}
    size: ${{ 4 if vars.env == 'prd' else 2 }}"
variables:
    env: prd
```
In this example, vars.env will be substituted in the expression assigned to cluster size
and the result will be 4.

Variables can even be used as keys of dictionary variables.

```yaml title="stack.yaml"
cluster:
  - name: pipeline-${vars.env}
    size: ${{ vars.sizes[vars.env] }}"
variables:
  env: prd
  sizes:
    dev: 2
    prd: 4
```

## Special cases

### Pipeline Nodes
When using SQL statements to define transformations for a DataFrame, it’s often necessary to reference the output of 
other [pipeline nodes](./pipeline.md) or the output of the previous transformer node. Laktory supports this by using {df} to reference 
the previous node’s DataFrame and {nodes.node_name} to reference the DataFrame from other specific pipeline nodes.

Here is an example:
```sql
SELECT 
    * 
FROM 
    {df} 
UNION 
    SELECT * FROM {nodes.node_01} 
UNION 
    SELECT * FROM {nodes.node_02}"
```

### Resources
What if the value of a variable needs to be the output of another deployed resource? Laktory supports resource variables 
to address this need.

Resource variables are declared using the notation `${resources.resource_name.resource_output}`. For instance, a job
could dynamically reference a deployed pipeline:

```yaml
name: my-job-${vars.env}
tasks:
  - task_key: pipeline
    pipeline_task:
      pipeline_id: ${resources.my-pipeline.id}
...
```
Here, the static pipeline ID is replaced by a dynamic reference to the pipeline resource `my-pipeline`.

Unlike user-defined variables, values for resource variables are populated automatically by Laktory. The available 
outputs correspond to those generated by the selected Infrastructure-as-Code backend (Pulumi or Terraform). For 
resource `x` to be available, it must be deployed as part of the current stack.

## Variable injection
??? "API Documentation"
    [`laktory.models.BaseModel.inject_vars`][laktory.models.BaseModel]<br>

Variable values are typically injected during deployment, just after serialization (`model_dump`). However, injection can 
also be triggered manually by invoking the `job.inject_vars()` method.