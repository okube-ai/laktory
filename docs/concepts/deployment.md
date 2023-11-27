Configuring models and validating them is a good start, but there is no added value if they can't be pushed to Databricks workspace.
Laktory also acts as an Infrastructure as Code (IaC) tool. 
In fact, it's built on top of IaC tools to run the deployment for you. 

Let's have a look at a simple example that deploy a pipeline with a single table using Pulumi.

Pipeline configuration
```yaml title="pipline.yaml"
name: pl-stock-prices

catalog: dev
target: finance

libraries:
  - notebook:
      path: /pipelines/dlt_brz_template.py

tables:
  - name: brz_stock_prices
    timestamp_key: data.created_at
    builder:
      layer: BRONZE
      event_source:
        name: stock_price
        producer:
          name: yahoo-finance
        read_as_stream: True
```
 
Pulumi main script
```py title="__main__.py"
from laktory import models

# Read configuration file
with open("pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)
    
# Deploy
pipeline.deploy_with_pulumi()
```
The `deploy_with_pulumi()` will trigger the instantiation of `pulumi_databricks.Pipeline(...)` class with all the required parameters.

Once you are ready to run the actual deploy, simply invoke
```cmd title="prompt"
pulumi up
```
as you would with any Pulumi project.

The same generally apply to Laktory models that have an equivalent Pulumi resource like notebook, grants, jobs, etc. 
A model that is deployable can be identified has having `laktory.models.resources.Resources` in its base classes (`Model.__bases__`)

## Resources Engines
Here is the list of resources engines (or IaC tools) that are currently support or will be supported in the future.
When they become available, it's simply about replacing the `deploy_with_pulumi` command in the example above with `deploy_with_xyz()`.

### Pulumi <img src="../../images/pulumi.png" alt="pulumi" width="16"/> 

[Pulumi](https://www.pulumi.com/) is currently the only fully supported IaC tool. It has been selected as it provides the best experience for data engineers and developers in general.

### Databricks SDK <img src="../../images/databricks.png" alt="databricks" width="16"/>

The next resources engine we have on the roadmap is the [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html) for python. 
Integration should be fairly straight forward as it supports most of the Databricks resources. 
However, given the non-declarative and stateless nature of this SDK, the feature set might be more limited. 
For example, you might be able to configure and deploy a new pipeline, but you might have to clean up manually the old 
one. 

### Terraform <img src="../../images/terraform.png" alt="terraform" width="16"/>
Given [Terraform](https://www.terraform.io/) popularity, we have no other choice but to include it on the list. 
Stay tuned for some expected release dates.