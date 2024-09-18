Laktory is built from the ground up to simplify and adhere to DataOps best practices.

For Databricks users, it provides an all-in-one solution for managing both Databricks resources—such as catalogs, 
schemas, users, groups, notebooks, and jobs—and data transformations, all through simple YAML files. This dramatically
simplifies workflows for teams that previously had to juggle a cumbersome mix of Terraform, Databricks Asset Bundles, 
and dbt. Additionally, Laktory speeds up iteration by enabling testing directly from your favorite IDE.

For non-Databricks users, Laktory still offers a robust ETL framework that can run locally or scale to environments like Kubernetes.

<img src="/../../images/dataops_diagram.png" alt="pipeline node" width="500"/>

## Declare
Declarative definitions for your data transformations using SQL and/or familiar dataframe operators.
```yaml title="pipeline_node.yaml"
name: slv_stock_prices
source:
  path: /Volumes/dev/sources/landing/tables/brz_stock_prices/
sink:
  schema_name: finance
  table_name: slv_stock_prices
transformer:
  nodes:
  - sql_expr: |
        SELECT
          data.created_at AS created_at,
          data.symbol AS symbol,
          data.open AS open,
          data.close AS close,
          data.high AS high,
          data.low AS low,
          data.volume AS volume
        FROM
          {df}
  - func_name: drop_duplicates
    func_kwargs:
      subset:
        - symbol
        - timestamp
...
```

Declarative definitions for your Unity Catalog objects with implicit grants attribution.
```yaml title="catalog.yaml"
catalog-dev:
  name: dev
  isolation_mode: OPEN
  grants:
    - principal: account users
      privileges:
        - USE_CATALOG
        - USE_SCHEMA
    - principal: metastore-admins
      privileges:
        - ALL_PRIVILEGES
  schemas:
    - name: laktory
      grants:
        - principal: laktory-friends
          privileges:
            - SELECT
    - name: sandbox
      grants:
        - principal: account users
          privileges:
            - SELECT
            - MODIFY
            - CREATE_FUNCTION
            - CREATE_MATERIALIZED_VIEW
            - CREATE_MODEL
            - CREATE_TABLE
```

## Validate
Validation and augmentation of configuration models, automatically applying 
default properties, templates, and custom Spark functions.
```py
from laktory import models

with open("pipeline_node.yaml") as fp:
    node = models.PipelineNode.model_validate(fp)
```

## Deploy
Deployment of resources to Databricks account and workspaces using the Laktory CLI, which integrates with
Infrastructure-as-Code (IaC) tools for seamless resource management.

```commandline title="command line"
laktory deploy --env dev
```

See [IaC](./iac.md) for more information.

## Operate
Execution of data pipelines on both local and remote hosts, with built-in error monitoring, either through Python code 
or the Laktory CLI.

#### Local testing
```py
from laktory import models

with open("pipeline.yaml") as fp:
    pl = models.Pipeline.model_validate(fp)

pl.execute(spark=spark)
```

#### Remote Testing
```commandline title="command line"
laktory run --job pl-stock-prices --env dev
```
