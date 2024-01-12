Configuring models and validating them is a good start, but what's the point if they can't be deployed to a Databricks workspace.
Laktory also acts as an Infrastructure as Code (IaC) tool. More accurately, it leverages IaC backends to manage the deployments.

## Backends
Here is the list of resources engines (or IaC tools) that are currently support or will be supported in the future.
When they become available, it's simply about replacing the `to_pulumi` command in the example above with `deploy_with_xyz()`.

### Pulumi <img src="../../images/pulumi.png" alt="pulumi" width="16"/> 

[Pulumi](https://www.pulumi.com/) is currently the only fully supported IaC tool. It has been selected as it provides the best experience for data engineers and developers in general.

[//]: # (### Databricks SDK <img src="../../images/databricks.png" alt="databricks" width="16"/>)

[//]: # ()
[//]: # (The next resources engine we have on the roadmap is the [Databricks SDK]&#40;https://docs.databricks.com/en/dev-tools/sdk-python.html&#41; for python. )

[//]: # (Integration should be fairly straight forward as it supports most of the Databricks resources. )

[//]: # (However, given the non-declarative and stateless nature of this SDK, the feature set might be more limited. )

[//]: # (For example, you might be able to configure and deploy a new pipeline, but you might have to clean up manually the old )

[//]: # (one. )

### Terraform <img src="../../images/terraform.png" alt="terraform" width="16"/>
Given [Terraform](https://www.terraform.io/) popularity, we have no other choice but to include it on the list. 
Stay tuned for some expected release dates.

## Pulumi - YAML

The simplest way of deploying resources is to declare a [stack](stack.md) in a yaml file and use laktory [CLI](cli.md)
to run the deployment.

```yaml title="stack.yaml"
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
```
 
To validate the configuration and preview the deployment, simply run
```cmd
laktory preview --stack my-org/dev
```

Here `--stack my-org/dev` is a pulumi-specific argument that defines the organization and the stack environment for
the deployment. The project name should match the `name` attributed in `stack.yaml`. Refer to pulumi [stacks](https://www.pulumi.com/learn/building-with-pulumi/understanding-stacks/)
for more information.

Once ready for deployment, simply run
```cmd
laktory deploy --stack my-org/dev
```

## Terraform - YAML

**Terraform backend is currently under development. Documentation will be updated once released.**

Switching to a terraform backend is as simple as changing the backend in the stack declared above.

```yaml title="stack.yaml"
name: workspace
backend: terraform
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
```

The CLI commands are the same, regardless of the selected backend, only the 
arguments might be slightly different.

Once ready for deployment, simply run
```cmd
laktory deploy --stack dev
```

## Pulumi - Python
If you prefer writing infrastructure as code with python, Laktory integrates nicely with Pulumi python.

```py title="__main__.py"
from laktory import models

# Declare pipeline
pipeline = models.Pipeline(
    name="pl-stock-prices",
    libraries=[{"notebooks": {"path": "/pipelines/dlt_brz_template.py"}}],
)

# Deploy
pipeline.to_pulumi()
```

The `to_pulumi()` will trigger the instantiation of `pulumi_databricks.Pipeline(...)` class with all the required parameters.

Once you are ready to run the actual deploy, simply invoke
```cmd title="prompt"
pulumi up
```
as you would with any Pulumi project. In this case, it is assumed that you have already setup a `Pulumi.yaml` file with
the required configuration for your stack.
