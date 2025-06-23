Getting started with Laktory is simple. Use the CLI quickstart command to set 
up a stack that aligns with the resources you want to deploy. You can choose 
from three templates, each tailored to specific deployment needs:

- `workflows`: A workspace-based deployment that includes notebooks, notebook jobs, Laktory job pipelines, and Laktory DLT pipelines.
- `workspace`: A deployment focused on your Databricks workspace, covering directories, secrets, and SQL warehouses.
- `unity-catalog`: A deployment targeting your Databricks account, including resources like groups, users, catalogs, and schemas.
- `local-pipeline`: A locally executable pipeline definition using Polars DataFrame engine for simplicity. 

After you followed the installation [instructions](install.md), open a command prompt and run the following command:
```cmd
laktory quickstart 
```
You will be prompted with two simple questions:

- Desired template: Choose between [workflows](#workspace-stack), [workspace](#workspace-stack), [unity-catalog](#unity-catalog-stack) and [local-pipeline](#local-pipeline-stack)
- Infrastructure as Code backend: Select either `terraform` or `pulumi` (except for `local-pipeline`)

Once these are selected, Laktory will generate the `stack.yaml` [file](concepts/stack.md), file, which acts as the main
entry point for declaring all your resources. The associated resources will also be created. Other than the `stack.yaml`
file, you are free to organize folder structures and file names as you see fit—there’s no strict convention to follow.

### Templates

- [Workflows](quickstarts/workflows.md)
- [Workspace](quickstarts/workspace.md)
- [Unity Catalog](quickstarts/unity-catalog.md)
- [Local Pipeline](quickstarts/local-pipeline.md)
