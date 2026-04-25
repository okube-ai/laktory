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
