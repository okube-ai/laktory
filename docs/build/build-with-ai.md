The fastest way to work with Laktory is through an AI coding agent. After running [`laktory setup-agent`](../concepts/agent.md), the agent has live access to model schemas and can generate, validate, and insert correct YAML directly into your project. The examples below illustrate the kinds of tasks you can describe in plain language.

## Data Pipeline

> Create a bronze pipeline node called `brz_orders` that reads JSONL files from `dbfs:/landing/orders/` and writes to a Unity Catalog table `dev.raw.orders` in OVERWRITE mode.

> Add a silver node `slv_orders` that reads from `brz_orders` (static), selects `order_id`, `customer_id`, and `CAST(created_at AS TIMESTAMP)`, drops duplicates on `(order_id, created_at)`, and writes to `dev.silver.orders`.

> Add a gold node `gld_orders_by_day` that aggregates `slv_orders` by `DATE(created_at)` and `customer_id`, computing `COUNT(order_id) AS order_count` and `SUM(amount) AS total_amount`.

> Change the source of `slv_orders` to streaming (`as_stream: true`) so the node only processes new records on each run.

> Add a custom transformer step in `slv_orders` that calls `lake.with_last_modified` from the `lake` wheel package. Wire up the dependency and import.

## Orchestration

> Configure the pipeline to run on a Lakeflow Job with serverless environment version 3, scheduled every day at 6am UTC.

> Switch from serverless to a dedicated cluster with 2–8 workers on `Standard_DS3_v2`, Spark 16.3, using `USER_ISOLATION` security mode.

> Add email notifications to the pipeline job that alert `data-team@example.com` on failure and `ops@example.com` on success.

## Resources

> Create a Unity Catalog named `dev` with `OPEN` isolation, grant `USE_CATALOG` and `USE_SCHEMA` to `account users`, and add two schemas: `finance` and `sandbox`.

> Define a group `data-engineers` with workspace USER permission, and a user `john.doe@example.com` who belongs to that group.

> Create a secret scope named `integrations` with a secret `api-token` and READ permission for `data-engineers`.

> Define a SQL warehouse named `analytics` — 2X-Small, serverless, auto-stop after 10 minutes, accessible by all users.

> Create a Databricks job `job-ingest` with two tasks: `ingest` (notebook `/jobs/ingest.py`) and `transform` (notebook `/jobs/transform.py`) that depends on `ingest`. Both run on a shared job cluster with 2 workers.
