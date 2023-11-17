# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

A DataOps framework for building Databricks lakehouse. 

![what_is_laktory](docs/images/what_is_laktory.png)


## What is it?
Laktory makes it possible to express and bring to life your data vision, from raw data to enriched analytics-ready datasets and finely tuned AI models, while adhering to basic DevOps best practices such as source control, code reviews and CI/CD.
By taking a declarative approach, you use configuration files or python code to instruct the desired outcome instead of detailing how to get there.
Such level of abstraction is made possible by the provided model methods, custom spark functions and templates.

Laktory is also your best friend when it comes to prototyping and debugging data pipelines. 
Within your workspace, you will have access to a custom `dlt` package allowing you to execute and test your notebook outside of a Delta Live Table execution.

Finally, Laktory got your testing and monitoring covered. 
A data pipeline built with Laktory is shipped with custom hooks enabling fine-grained monitoring of performance and failures [under development]    

## Who is it for?
Laktory is not web app or a UI that you can use to visually build your data pipeline. 
We don't promise that you will be up and running within a few clicks.
You need basic programming or DevOps experience to get started. 
What we do promise on the other hand is that once you are past the initial setup, you will be able to efficiently scale, deploying hundreds of datasets and models without compromising data governance.   


## Help
TODO: Build full help documentation

## Installation
Install using `pip install laktory`

TODO: Full installation instructions

## A Basic Example
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

## A full Data Ops template
A comprehensive template on how to deploy a lakehouse as code using Laktory is maintained here:
https://github.com/okube-ai/lakehouse-as-code.

In this template, 4 pulumi projects are used to:
- `{cloud_provider}_infra`: Deploy the required resources on your cloud provider
- `unity-catalog`: Setup users, groups, catalogs, schemas and manage grants
- `workspace-conf`: Setup secrets, clusters and warehouses
- `workspace`: The data workflows to build your lakehouse.

## Okube Company
Okube is dedicated to build open source frameworks, the *kubes*, that empower businesses to build and deploy highly scalable data platforms and AI models. Contributions are more than welcome.

