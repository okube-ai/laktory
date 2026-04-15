Getting started with Laktory is simple. Use the CLI quickstart command to set 
up a stack that aligns with the resources you want to deploy. You can choose 
from several templates, each tailored to specific deployment needs:

- `workflows-dab`: Sample Laktory pipelines for use with Databricks Asset Bundles. Configure with YAML. Deploy with DAB.
- `workflows`: A workspace-based deployment that includes notebooks, notebook jobs and spark-based Laktory job/DLT pipelines.
- `workspace`: A deployment focused on your Databricks workspace, covering directories, secrets, and SQL warehouses.
- `unity-catalog`: A deployment targeting your Databricks account, including resources like groups, users, catalogs, and schemas.
- `local-pipeline`: A polars-based pipeline that can be executed locally.

After you followed the installation [instructions](../install.md), open a command prompt and run the following command:
```cmd
laktory quickstart 
```
For the `workflows-dab` and `local-pipeline` templates, no IaC backend selection is needed.
For all other templates, you will also be prompted to select an Infrastructure as Code backend:
`terraform` or `pulumi`.

### Templates

- [Workflows DAB](./workflows-dab.md)
- [Workflows](./workflows.md)
- [Workspace](./workspace.md)
- [Unity Catalog](./unity-catalog.md)
- [Local Pipeline](./local-pipeline.md)
