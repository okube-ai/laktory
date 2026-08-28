??? "API Documentation"
    [`laktory.models.Stack`][laktory.models.Stack]<br>

The stack is the main entry point for Laktory and acts as a container for 
resources, while also serving as a configuration object for deployment across 
multiple environments.


```yaml
name: workspace
resources:
  pipelines:
    pl-stock-prices:
      name: pl-stock-prices
      libraries:
        - notebook:
            path: /pipelines/laktory_ldp.py
  jobs:
    job-stock-prices:
      name: job-stock-prices
      clusters:
        - name: main
          spark_version: 16.3.x-scala2.12
          node_type_id: Standard_DS3_v2      
      tasks:
          - task_key: ingest
            job_cluster_key: main
            notebook_task:
              notebook_path: /.laktory/jobs/ingest_stock_prices.py
variables:
  org: okube

environments:
  dev:
    resources:
      pipelines:
        pl-stock-prices:
          development: True    
  prod:
    resources:
      pipelines:
        pl-stock-prices:
          development: False

terraform:
  backend:
    local:
        path: terraform.tfstate

```
### Resources
The `resources` attribute lists the Laktory models or resources to be deployed. This is structured as nested
dictionaries with three levels: `resource_type.resource_name.resource_properties`.

### Variables
The `variables` attribute declares variables that can be used to parameterize a model declaration. More details can be found [here](variables.md).

### Environments
The `environments` attribute defines environment-specific properties for `config`, `resources`, or `variables`. Each
environment is structured similarly to the root and overwrites the default values at the root level.

For example, both the `dev` and `prod` environments will include a pipeline named `pl-stock-prices` with an associated 
notebook. However, in the `dev` environment, the `development` property will be set to `True`.

Each environment will be deployed as a standalone set of resources or stack.

### Settings
The `settings` attribute configures Laktory-wide behavior for this stack. The most commonly used is `workspace_root` - where deployed objects like notebooks and workspace files land by default (see [Workspace Root](workspaceroot.md)) - alongside `build_root`, `runtime_root`, `dataframe_backend`, and `dataframe_api`.

```yaml
settings:
  workspace_root: user_root
```

Settings values can reference [variables](variables.md) via `${vars.x}`, and are themselves reusable elsewhere in the stack via `${settings.x}` (see [Variables - Settings](variables.md#settings)). See [`LaktorySettings`][laktory.models.stacks.stack.LaktorySettings] in the API reference for the full list of fields.

### Backend configuration
The `terraform` block attributes define the Infrastructure-as-Code (IaC) configuration, and how to 
configure resource providers (such as Azure, AWS, GCP, Databricks) for secure access.

Setting `terraform.backend.databricks_workspace: true` auto-configures a Terraform state backend scoped to your own Databricks user directory, stack name, and environment - no separate cloud storage account needed. See [Workspace Root](workspaceroot.md) for the full details, including how to keep it consistent with where deployed objects (notebooks, workspace files, ...) land.

