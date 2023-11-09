# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

A DataOps framework for building Databricks lakehouse.

## Okube Company 

# 

Okube is dedicated to build open source frameworks, the *kubes*, that empower businesses to build and deploy highly scalable data platforms and AI models. Contributions are more than welcome.


## Help
TODO: Build full help documentation

## Installation
Install using `pip install laktory`

TODO: Full installation instructions

### pyspark
Optionally, you can also install spark locally to test your custom functions.

TODO: Add pyspark instructions https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/
- JAVA_HOME=/opt/homebrew/opt/java;
- SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec


## A Basic Example
This example demonstrates how to send data events to a data lake and to set a
data pipeline defining the tables transformation layers. 

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

### Define data pipeline and data tables
A yaml file define the configuration for a data pipeline, including the transformations of a raw data event into curated
(silver) and consumption (gold) layers.

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
      path: /pipelines/dlt_template_brz.py
  - notebook:
      path: /pipelines/dlt_template_slv.py

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
    event_source:
      name: stock_price
      producer:
        name: yahoo-finance
    zone: BRONZE


  - name: slv_stock_prices
    table_source:
      catalog_name: ${var.env}
      schema_name: finance
      name: brz_stock_prices
    zone: SILVER
    columns:
      - name: created_at
        type: timestamp
        spark_func_name: coalesce
        spark_func_args:
          - data._created_at

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

      - name: high
        type: double
        sql_expression: GREATEST(data.open, data.close)

```

### Deploy your configuration
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
## A full Data Ops template
A comprehensive template on how to deploy a lakehouse as code using Laktory is maintained here:
https://github.com/okube-ai/lakehouse-as-code.

In this template, 4 pulumi projects are used to:
- `{cloud_provider}_infra`: Deploy the required resources on your cloud provider
- `unity-catalog`: Setup users, groups, catalogs, schemas and manage grants
- `workspace-conf`: Setup secrets, clusters and warehouses
- `workspace`: The data workflows to build your lakehouse.
