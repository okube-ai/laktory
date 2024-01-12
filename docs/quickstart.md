Suppose your end goal is to compare Apple and Google stock performances.
The example below illustrates how Laktory can help with:

* Generating the raw data
* Declaring the transformation layers (bronze, silver, gold)
* Deploying the corresponding data pipeline to Databricks


### Generate data events

??? "API Documentation"
    [`laktory.models.DataEvent`][laktory.models.DataEvent]<br>

A `DataEvent` class helps you set both the metadata (event name, producer, etc.) and the data of an event and provides the methods for writing that event to a cloud storage or databricks volume/mount.

```py
from laktory import models
from datetime import datetime


events = [
    models.DataEvent(
        name="stock_price",
        producer={
            "name": "yahoo-finance",
        },
        data={
            "created_at": datetime(2023, 8, 23),
            "symbol": "GOOGL",
            "open": 130.25,
            "close": 132.33,
        },
    ),
    models.DataEvent(
        name="stock_price",
        producer={
            "name": "yahoo-finance",
        },
        data={
            "created_at": datetime(2023, 8, 24),
            "symbol": "GOOGL",
            "open": 132.00,
            "close": 134.12,
        },
    ),
]

# Export to databricks landing volume / mount.
for event in events:
    event.to_databricks()
```

### Declare data pipeline and tables
??? "API Documentation"
    [`laktory.models.Table`][laktory.models.Table]<br>
    [`laktory.models.Pipeline`][laktory.models.Pipeline]<br>

Once you have data events in your landing storage (they can be generated with any external system), build a yaml file (or python code) to define your data pipeline and the associated transformations. This configuration file may be used to set

* pipeline properties
* data transformations
* privileges and grants

```yaml title="pipeline.yaml"
name: pl-stock-prices

catalog: ${vars.env}
target: default

clusters:
  - name : default
    node_type_id: Standard_DS3_v2
    autoscale:
      min_workers: 1
      max_workers: 2

libraries:
  - notebook:
      path: /pipelines/dlt_brz_template.py
  - notebook:
      path: /pipelines/dlt_slv_template.py
  - notebook:
      path: /pipelines/dlt_gld_stock_performances.py

permissions:
  - group_name: account users
    permission_level: CAN_VIEW
  - group_name: role-engineers
    permission_level: CAN_RUN

# --------------------------------------------------------------------------- #
# Tables                                                                      #
# --------------------------------------------------------------------------- #

tables:
  - name: brz_stock_prices
    timestamp_key: data.created_at
    builder:
      layer: BRONZE
      event_source:
        name: stock_price
        producer:
          name: yahoo-finance


  - name: slv_stock_prices
    timestamp_key: created_at
    builder:
      layer: SILVER
      table_source:
        name: brz_stock_prices
    columns:
      - name: created_at
        type: timestamp
        spark_func_name: coalesce
        spark_func_args:
          - data._created_at

      - name: symbol
        type: string
        spark_func_name: coalesce
        spark_func_args:
          - data.symbol

      - name: open
        type: double
        spark_func_name: coalesce
        spark_func_args:
          - data.open

      - name: close
        type: double
        spark_func_name: coalesce
        spark_func_args:
          - data.close
```

### Validate and deploy
Now that your pipeline is defined, it (along with other resources) can be included in a stack and deployed using laktory CLI.

```yaml title="stack.yaml"
name: my-stack
backend: pulumi
config:
  databricks:host: ${vars.DATABRICKS_HOST}
  databricks:token: ${vars.DATABRICKS_TOKEN}
resources:
  pipelines:
    pl-stock-prices: ${include.pipeline.yaml}
environments:
  dev:
    variables:
      env: dev
  prod:
    variables:
      env: prod
```

Deploy with laktory CLI
```cmd
laktory deploy --stack my-organization/dev
```
As you may have noticed from the stack definition, pulumi is used as the IaC backend. Currently, only pulumi is supported,
but terraform in the near future.

### Run your pipeline
Once deployed, you pipeline is ready to be run or will be run automatically if it's part of a scheduled job.
![pl-stock-prices](images/pl_stock_prices_simple.png)


### Debug your pipeline
If you need to debug or modify one of your pipeline's notebook, Laktory makes it very easy by allowing you to run and inspect (with some limitations) the output data outside of the DLT pipeline.

```py title="dlt_slv_template.py"
from laktory import dlt
from laktory import read_metadata
from laktory import get_logger

dlt.spark = spark
logger = get_logger(__name__)

# Read pipeline definition
pl_name = spark.conf.get("pipeline_name", "pl-stock-prices")
pl = read_metadata(pipeline=pl_name)


# Define table
def define_table(table):
    @dlt.table(name=table.name, comment=table.comment)
    def get_df():
        logger.info(f"Building {table.name} table")

        # Read Source
        df = table.builder.read_source(spark)
        df.printSchema()

        # Process
        df = table.builder.process(df, spark=spark)

        # Return
        return df

    return get_df


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

# Build tables
for table in pl.tables:
    if table.layer == "SILVER":
        wrapper = define_table(table)
        df = dlt.get_df(wrapper)
        display(df)
```

Output:

![pl-stock-prices](images/dlt_debug.png)