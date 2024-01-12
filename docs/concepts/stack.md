??? "API Documentation"
    [`laktory.models.Stack`][laktory.models.Stack]<br>
Depending on your deployment strategy and/or backend, you might need to include the model or resources as part of a Stack

The stack act as a container for the resources, but also as a configuration object for your deployment through multiple environments.


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
The attributes `name`, `backend` and `config` define the IaC backend to use and how the different resources providers
(azure, aws, gcp, databricks, etc) should be configured for secure access.

### Resources
The `resources` attribute is the list of laktory models or resources to be deployed. It's structured as nested dictionaries
with the following 3 levels `resource_type`.`resource_name`.`resource_properties`

### Variables
The `variables` attribute declares a list of variables that can be used to parametrize a model declaration. More 
details [here](variables.md).

### Environments
The `environments` attribute define environment-specific properties for `config`, `resources` or `variables`. 
The structure is the same as the root structure and each statement is an overwrite to the default value defined at the
root. 

For example, in this case, both `dev` and `prod` environment will be populated with a pipeline named `pl-stock-prices`
having 1 associated notebook, but only in `dev` environment will the `development` property set to `True`.

Each environment will be deployed as a standalone set of resources or stack
