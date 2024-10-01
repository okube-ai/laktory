The `workflows` stack sets up and deploys three key components: a "Hello World!" Databricks job, a Laktory pipeline for
stock prices (as a Databricks Job and as a Delta Live Table), and all the necessary supporting resources.

### Create Stack
To create the stack, use the following command:
```commandline
laktory quickstart -t workflows
```

#### Files
After running the quickstart command, the following structure is created:

```bash
.
├── data
│   └── stock_prices.json
├── notebooks
│   ├── dlt
│   │   └── dlt_laktory_pl.py
│   └── jobs
│       ├── job_hello.py
│       └── job_laktory_pl.py
├── read_env.sh
├── requirements.txt
├── resources
│   ├── dbfsfiles.yaml
│   ├── job-hello.yaml
│   ├── notebooks.yaml
│   ├── pl-stocks-spark-dlt.yaml
│   └── pl-stocks-sql.yaml
├── sql
│   └── slv_stock_prices.sql
├── stack.yaml
```

#### Resources Directory
The files in the `resources` directory are referenced in the `stack.yaml` file and declare various resources to be 
deployed. Each file specifies one or more resources.

#### Notebooks Directory
The `notebooks` directory contains the notebooks to be deployed to the Databricks workspace, as defined in the 
`resources/notebooks.yaml` file. These notebooks are also referenced in job and pipeline definitions.

#### Data Directory
For the pipeline to run, a sample `stock_prices.json` data file is provided and will be uploaded to DBFS as declared in 
`resources/dbfsfiles.yaml`.

### Set Environment Variables
Before deployment, ensure the following environment variables are properly set, as referenced in the `stack.yaml` file:

- `DATABRICKS_HOST`: The URL of your Databricks workspace
- `DATABRICKS_TOKEN`: A valid Databricks [personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html)

### Deploy Stack
Now you are ready to deploy your stack. If you're using Terraform, start by initializing the environment:

```cmd
laktory init --env dev
```

Then, deploy the stack:
```cmd
laktory deploy --env dev
```

The deployment process will refresh the state of your Databricks resources and generate an execution plan using 
Terraform. Once the plan is generated, simply confirm by typing "yes" to proceed.

<div class="code-output">
```cmd
(laktory) osoucy@countach workflows % laktory deploy --env dev
databricks_dbfs_file.dbfs-file-stock-prices: Refreshing state... [id=/Workspace/.laktory/data/stock_prices/stock_prices.json]
databricks_notebook.notebook-job-laktory-pl: Refreshing state... [id=/.laktory/jobs/job_laktory_pl.py]
databricks_workspace_file.workspace-file-laktory-pipelines-pl-stocks-spark-dlt-json: Refreshing state... [id=/.laktory/pipelines/pl-stocks-spark-dlt.json]
databricks_workspace_file.workspace-file-laktory-pipelines-pl-stocks-sql-json: Refreshing state... [id=/.laktory/pipelines/pl-stocks-sql.json]
databricks_notebook.notebook-dlt-laktory-pl: Refreshing state... [id=/.laktory/dlt/dlt_laktory_pl.py]
databricks_notebook.notebook-job-hello: Refreshing state... [id=/.laktory/jobs/job_hello.py]
databricks_pipeline.pl-stocks-spark-dlt: Refreshing state... [id=dc28dbfb-0407-42fa-979f-94e17fa1fd30]
databricks_job.job-hello: Refreshing state... [id=808120666287598]
databricks_job.pipeline-databricks-job-job-pl-stock-sql: Refreshing state... [id=210130692451027]
databricks_permissions.permissions-job-hello: Refreshing state... [id=/jobs/808120666287598]
databricks_permissions.permissions-workspace-file-laktory-pipelines-pl-stocks-spark-dlt-json: Refreshing state... [id=/files/4435054900343469]
databricks_permissions.permissions-workspace-file-laktory-pipelines-pl-stocks-sql-json: Refreshing state... [id=/files/4435054900343468]

Note: Objects have changed outside of Terraform

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create
-/+ destroy and then create replacement

Terraform will perform the following actions:

  # databricks_dbfs_file.dbfs-file-stock-prices will be created
  + resource "databricks_dbfs_file" "dbfs-file-stock-prices" {
      + dbfs_path = (known after apply)
      + file_size = (known after apply)
      + id        = (known after apply)
      + md5       = "different"
      + path      = "/Workspace/.laktory/data/stock_prices/stock_prices.json"
      + source    = "./data/stock_prices.json"
    }
    [...]

Plan: 12 to add, 0 to change, 1 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes
```
</div>

<br>
After deployment, you can check the Databricks workspace to confirm that the pipeline job has been successfully deployed.
![pl-stock-prices](../images/job_pl_stock_sql.png)


### Run Pipeline Job

You can now run your pipeline either from the Databricks UI or using the Laktory CLI:

```cmd
laktory run --env dev --job pl-stock-sql
```
This will start the pipeline run and provide real-time status updates for each task in the job. The output will show the
job's progress, including any tasks that are pending, running, or completed.

<div class="code-output">
```cmd
(laktory) osoucy@countach workflows % laktory run --env dev --job job-pl-stock-sql
INFO - Getting id for job job-pl-stock-sql
INFO - Getting id for pipeline pl-stocks-spark-dlt
INFO - Getting id for job job-hello
INFO - Job job-pl-stock-sql run started...
INFO - Job job-pl-stock-sql run URL: https://adb-2211091707396001.1.azuredatabricks.net/?o=2211091707396001#job/977904116405742/run/1052232433102036
INFO - Job job-pl-stock-sql state: RUNNING
INFO -    Task job-pl-stock-sql.node-brz_stock_prices_sql state: PENDING
INFO -    Task job-pl-stock-sql.node-slv_stock_prices_sql state: BLOCKED
INFO -    Task job-pl-stock-sql.node-brz_stock_prices_sql state: RUNNING
INFO -    Task job-pl-stock-sql.node-brz_stock_prices_sql state: TERMINATED
INFO -    Task job-pl-stock-sql.node-slv_stock_prices_sql state: PENDING
INFO -    Task job-pl-stock-sql.node-slv_stock_prices_sql state: RUNNING
INFO -    Task job-pl-stock-sql.node-slv_stock_prices_sql state: TERMINATING
INFO - Job job-pl-stock-sql state: TERMINATED
INFO -    Task job-pl-stock-sql.node-slv_stock_prices_sql state: TERMINATED
INFO - Job job-pl-stock-sql run terminated after 232.99 sec with RunLifeCycleState.TERMINATED 
INFO - Task job-pl-stock-sql.node-brz_stock_prices_sql terminated with RunResultState.SUCCESS 
INFO - Task job-pl-stock-sql.node-slv_stock_prices_sql terminated with RunResultState.SUCCESS 
```
</div>
