While Laktory offers a simplified interface for declaring and deploying Databricks resources, it is built on top of 
Infrastructure as Code (IaC) backends to provide a robust and proven experience.

## Backends

### Declarative Automation Bundles (DAB) <img src="../../images/logos/databricks.png" alt="databricks" width="16"/>

[Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) is Databricks' native 
IaC solution, built directly into the Databricks CLI. Laktory integrates with DAB through a Python resource hook: 
declare your pipelines as individual YAML files, register the hook in `databricks.yml`, and run 
`databricks bundle deploy`. Laktory generates the required Job and DLT Pipeline resources automatically — no 
`stack.yaml` file required.

If your team already uses the Databricks CLI and DAB to manage workspace resources, this is the fastest path to 
getting Laktory pipelines deployed. See the [DAB concept page](dab.md) for full details.


### Terraform <img src="../../images/logos/terraform.png" alt="terraform" width="16"/>
[Terraform](https://www.terraform.io/) is one of the most popular IaC tools available. It allows users to define and 
provision infrastructure resources using a declarative configuration language, automating the creation and management 
of both cloud and on-premises infrastructure. While Laktory currently supports many Terraform features, additional 
capabilities may be added in the future.


## Terraform
Deploying resources is achieved by creating a [stack](stack.md) in a yaml file and using laktory [CLI](cli.md)
to run the deployment.

```yaml title="stack.yaml"
name: workspace
resources:
  providers:
    databricks:
      host: ${vars.DATABRICKS_HOST}
      token: ${vars.DATABRICKS_TOKEN}
  pipelines:
    pl-stock-prices:
      name: pl-stock-prices
      libraries:
        - notebook:
            path: /pipelines/dlt_brz_template.py
```

To validate the configuration and preview the deployment, run:
```cmd
laktory preview
```

Once ready to deploy, use:
```cmd
laktory deploy
```

If your stack defines multiple environments, you can target a specific environment with:
```cmd
laktory deploy --env dev
```

### State management

Terraform tracks deployed resources in a **state file**. By default the state is stored locally, which works for a single developer but is not suitable for teams or CI/CD pipelines. Laktory's `terraform.backend` field accepts any [Terraform backend configuration](https://developer.hashicorp.com/terraform/language/backend).

#### Databricks workspace backend

For Databricks users, Laktory provides a built-in shortcut that stores the state file directly in the Databricks workspace — the same mechanism used by Databricks Asset Bundles (DABs). No external storage account or additional credentials are required.

```yaml title="stack.yaml"
terraform:
  backend:
    databricks_workspace: true
```

Laktory stores state at:

```
/Users/{current-user}/.laktory/{stack-name}/{env}/state/terraform.tfstate
```

Credentials are taken automatically from the `DatabricksProvider` already defined in your stack. Both PAT tokens and service principals (`client_id`/`client_secret` or Azure equivalents) are supported.

#### Other backends

Any standard Terraform backend is also supported — just provide the backend block directly:

```yaml title="stack.yaml"
terraform:
  backend:
    azurerm:
      resource_group_name: my-rg
      storage_account_name: mystorageaccount
      container_name: terraform
      key: states/my-stack.tfstate
```



## Declarative Automation Bundles
If your team already uses the Databricks CLI, DAB integration requires only a few additions to your existing 
`databricks.yml`.

```yaml title="databricks.yml"
variables:
  dab_workspace_root:
    default: ${workspace.root_path}
  laktory_pipelines_dir:
    default: ./laktory/pipelines

sync:
  paths:
    - ./laktory
  include:
    - ./laktory/.build/**

python:
  venv_path: .venv
  resources:
    - 'laktory.dab:build_resources'
```

Each pipeline YAML file in `laktory_pipelines_dir` is loaded automatically. Laktory generates the corresponding 
Databricks Job or DLT Pipeline resource and writes the pipeline config to `laktory/.build/` for DAB to sync to the 
workspace. Then deploy as usual:

```cmd
databricks bundle deploy --target dev
```

See the [DAB concept page](dab.md) for full details including orchestrator options, variable injection, and settings.

---

## Resource names

Every resource in a Laktory stack has a **logical name** that serves two purposes:

1. **State tracking** — the IaC backend uses it to identify the resource across deployments. Changing the name is treated as a deletion + recreation.
2. **Cross-referencing** — other resources can reference this resource's attributes using `${resources.<name>.<property>}` (e.g., `${resources.catalog-dev.id}`).

### Auto-generated names

If you don't set a name explicitly, Laktory generates one from the resource type and its `name` field:

| Resource | Auto-generated name |
|----------|---------------------|
| `Catalog(name="dev")` | `catalog-dev` |
| `Schema(name="finance", catalog_name="dev")` | `schema-dev-finance` |
| `Table(name="slv_stock_prices", catalog_name="dev", schema_name="finance")` | `table-dev-finance-slv_stock_prices` |
| `SecretScope(name="my-scope")` | `secret-scope-my-scope` |

The pattern is `{resource-type}-{name}` where the name is flattened from all identity fields and special characters are replaced with `-`.

### Cross-referencing resources

Use `${resources.<name>.<property>}` to inject a resource attribute into another resource's field:

```yaml title="stack.yaml"
resources:
  catalogs:
    my-catalog:
      name: dev

  schemas:
    my-schema:
      name: finance
      catalog_name: ${resources.catalog-dev.id}
```

Common properties are `.id`, `.name`, and `.full_name`. The available properties depend on the resource type.

### Pinning a name

If you need to control the exact name — for example, to keep a stable reference when the `name` field changes, or to use a shorter key for frequent cross-references — set `resource_options.name`:

```yaml title="stack.yaml"
resources:
  catalogs:
    my-catalog:
      name: dev
      resource_options:
        name: main-catalog   # cross-reference as ${resources.main-catalog.id}
```

The value must start with a letter and contain only letters, digits, hyphens, and underscores.

---

## Importing existing resources

`lookup_existing` lets you import a pre-existing Databricks resource into your stack without recreating it. When set, Laktory reads the resource's state from the platform and makes it available for cross-referencing (`${resources.<name>.<property>}`) and child resource deployment (grants, schemas, etc.). The resource's own field values are not written back to the existing resource.

```yaml title="stack.yaml"
resources:
  groups:
    my-group:
      display_name: admins
      lookup_existing:
        display_name: admins
```

Which fields to provide in `lookup_existing` depends on the resource type — each resource's API reference documents the available lookup key(s) on its `lookup_existing` field.
