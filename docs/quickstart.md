Suppose you need to compare stock prices performance, this example demonstrates how Laktory can help you with each step of the process, from data ingestion to aggregated analytics.


### Generate data events
A data event class defines specifications of an event and provides methods
for writing that event directly to a cloud storage or through a databricks volume or mount.

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
    )
]

for event in events:
    event.to_databricks()
```

### Declare data pipeline and data tables
A yaml file define the configuration for a data pipeline, including the transformations of a raw data event into silver (curated) and gold (consumption) layers.

```yaml
name: pl-stock-prices

catalog: ${var.env}
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
      path: /pipelines/dlt_slv_star_template.py
  - notebook:
      path: /pipelines/dlt_gld_stock_prices.py
  - notebook:
      path: /pipelines/dlt_gld_stock_performances.py

udfs:
  - module_name: stock_functions
    function_name: symbol_to_name

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
      zone: BRONZE
      event_source:
        name: stock_price
        producer:
          name: yahoo-finance


  - name: slv_stock_prices
    timestamp_key: created_at
    builder:
      zone: SILVER
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

      - name: name
        type: string
        spark_func_name: symbol_to_name
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

      - name: low
        type: double
        spark_func_name: coalesce
        spark_func_args:
          - data.low

      - name: high
        type: double
        spark_func_name: coalesce
        spark_func_args:
          - data.high

  - name: brz_stock_metadata
    builder:
      zone: BRONZE
      event_source:
        name: stock_metadata
        read_as_stream: False
        producer:
          name: yahoo-finance

  - name: slv_stock_metadata
    builder:
      zone: SILVER
      table_source:
        name: brz_stock_metadata
        read_as_stream: False
    columns:
      - name: symbol
        type: string
        spark_func_name: coalesce
        spark_func_args:
          - data.symbol

      - name: currency
        type: string
        spark_func_name: coalesce
        spark_func_args:
          - data.currency

      - name: first_traded
        type: timestamp
        spark_func_name: coalesce
        spark_func_args:
          - data.firstTradeDate

  - name: slv_star_stock_prices
    builder:
      zone: SILVER_STAR
      table_source:
        name: slv_stock_prices
        read_as_stream: True
      joins:
        - other:
            name: slv_stock_metadata
            read_as_stream: False
            selects:
              - symbol
              - currency
              - first_traded
          "on":
            - symbol
```

### Instantiate and deploy
Laktory currently support Pulumi for cloud deployment, but more engines will be added in the future (Terraform, Databricks CLI, etc.).

```py
import os
import pulumi
from laktory import models

# Read configuration file
with open("pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

# Set variables
pipeline.vars = {
    "env": os.getenv("ENV"),
}
    
# Deploy
pipeline.deploy_with_pulumi()
```

From the terminal,
```cmd
pulumi up
```

### Run your pipeline
Once deployed, you pipeline is ready to be run or will be run automatically if it's part of a scheduled job.
![pl-stock-prices](docs/images/pl_stock_prices.png)
