While Laktory offers a simplified interface for declaring and deploying Databricks resources, it is built on top of 
Infrastructure as Code (IaC) backends to provide a robust and proven experience.

## Backends

### Terraform <img src="../../images/terraform.png" alt="terraform" width="16"/>
[Terraform](https://www.terraform.io/) is one of the most popular IaC tools available. It allows users to define and 
provision infrastructure resources using a declarative configuration language, automating the creation and management 
of both cloud and on-premises infrastructure. While Laktory currently supports many Terraform features, additional 
capabilities may be added in the future.


### Pulumi <img src="../../images/pulumi.png" alt="pulumi" width="16"/> 

[Pulumi](https://www.pulumi.com/) is gaining momentum as a flexible IaC tool, supporting multiple programming languages 
such as Python, Node.js, Go, and Java. In the context of Laktory, Pulumi offers a strong alternative by enabling the use
of Python for configuration, instead of a YAML file. However, only YAML is currently supported through Laktory.

## Terraform
Deploying resources is achieved by creating a [stack](stack.md) in a yaml file and using laktory [CLI](cli.md)
to run the deployment.

```yaml title="stack.yaml"
name: workspace
backend: terraform
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



## Pulumi
To use Pulumi as the backend, simply change the backend field and configure the default providers using the config field.

```yaml title="stack.yaml"
name: workspace
organization: okube
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
 
The CLI commands remain the same regardless of the backend, with slight differences in the arguments.

When using Pulumi, the organization must be specified in the stack file or during execution, along with the environment. 
This fully qualifies the underlying Pulumi stack.
```cmd
laktory preview --org okube --env dev
```
This command is equivalent to running `pulumi preview --stack okube/dev`. Ensure that the Pulumi project name matches the
`name` specified in the `stack.yaml` file.

Refer to pulumi [stacks](https://www.pulumi.com/learn/building-with-pulumi/understanding-stacks/)
for more information.
