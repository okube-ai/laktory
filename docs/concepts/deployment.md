Configuring models and validating them is a good start, but what's the point if they can't be deployed to a Databricks
workspace? Laktory also provides Infrastructure as Code (IaC) capabilities by leveraging IaC backends.

## Backends

### Terraform <img src="../../images/terraform.png" alt="terraform" width="16"/>
[Terraform](https://www.terraform.io/) is one of the most popular IaC tools on the market. The open-source platform 
allows users to define and provision infrastructure resources using a declarative configuration language, facilitating
the automated creation and management of cloud and on-premises infrastructure. Not all features are supported through
Laktory, but more might be added in the future.


### Pulumi <img src="../../images/pulumi.png" alt="pulumi" width="16"/> 

[Pulumi](https://www.pulumi.com/) might not be as popular as Terraform, but it has gained a lot of momentum in the last
few months. It's defining characteristic is its support for multiple programming languages, like Python, Node.js, Go 
and Java. In the context of Laktory, it offers a nice alternative as it allows to leverage Python to declare the 
configuration instead of a simple yaml file. Only YAML is supported through Laktory.


## Terraform
The simplest way of deploying resources is to declare a [stack](stack.md) in a yaml file and use laktory [CLI](cli.md)
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

To validate the configuration and preview the deployment, simply run
```cmd
laktory preview
```

Once ready for deployment, simply run
```cmd
laktory deploy
```

If your stack defines multiple environments, you can select the target 
environment with
```cmd
laktory deploy --env dev
```



## Pulumi
Using Pulumi backend simply requires to change the backend and to configure
default providers using the config field.

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
 
The CLI commands are the same, regardless of the selected backend, only the 
arguments might be slightly different.

With Pulumi backend, the organization must be provided within the stack file
or when running a `laktory` command. The environment must also be specified.
This is required to full qualify the underlying Pulumi stack.
```cmd
laktory preview --org okube --env dev
```
The above command would be analogous to running `pulumi preview --stack okube/dev`. Also note that the Pulumi project
name should match the `name` attributed in `stack.yaml`.

Refer to pulumi [stacks](https://www.pulumi.com/learn/building-with-pulumi/understanding-stacks/)
for more information.
