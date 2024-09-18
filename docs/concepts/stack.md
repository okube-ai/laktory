??? "API Documentation"
    [`laktory.models.Stack`][laktory.models.Stack]<br>

The stack is the main entry point for Laktory and acts as a container for 
resources, while also serving as a configuration object for deployment across 
multiple environments.


```yaml
name: workspace
backend: pulumi
config:
  databricks:host: ${vars.DATABRICKS_HOST}
  databricks:token: ${vars.DATABRICKS_TOKEN}
resources:
  pipelines:
    pl-stock-prices:
      name: pl-stock-prices
      libraries:
        - notebook:
            path: /pipelines/dlt_brz_template.py
  jobs:
    job-stock-prices:
      name: job-stock-prices
      clusters:
        - name: main
          spark_version: 14.0.x-scala2.12
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
```
### Backend configuration
The `name`, `backend`, and `config` attributes define the Infrastructure-as-Code (IaC) backend to use, and how to 
configure resource providers (such as Azure, AWS, GCP, Databricks) for secure access.

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