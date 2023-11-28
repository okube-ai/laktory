# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![test](https://github.com/okube-ai/laktory/actions/workflows/test.yml/badge.svg)](https://github.com/okube-ai/laktory/actions/workflows/test.yml)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

A DataOps framework for building Databricks lakehouse.

<img src="docs/images/what_is_laktory.png" alt="what is laktory" width="400"/>

Laktory makes it possible to express and bring to life your data vision, from raw to enriched analytics-ready datasets and finely tuned AI models, while adhering to basic DevOps best practices such as source control, code reviews and CI/CD.

Using a declarative approach, you define your datasets and transformations, validate them and deploy them into Databricks workspaces. 
Once deployed, you can once again leverage Laktory for debugging and monitoring.  

## Help
See [documentation](https://www.laktory.ai/) for more details.

## Installation
Install using 
```commandline
pip install laktory[{cloud_provider}]
```
where `{cloud_provider}` is `azure`, `aws` or `gcp`. 

For more installation options,
see the [Install](https://www.laktory.ai/install/) section in the documentation.

## A Basic Example
```py
from laktory import models

table = models.Table(
    name="brz_stock_prices",
    catalog_name="prod",
    schema_name="finance",
    timestamp_key="data.created_at",
    builder={
        "layer": "SILVER",
        "table_source": {
            "name": "brz_stock_prices",
        }
    },
    columns=[
        {
            "name": "symbol",
            "type": "string",
            "sql_expression": "data.symbol"
        }
    ]
)

print(table)
#> catalog_name='prod' columns=[Column(catalog_name='prod', comment=None, name='symbol', pii=None, schema_name='finance', spark_func_args=[], spark_func_kwargs={}, spark_func_name=None, sql_expression='data.symbol', table_name='brz_stock_prices', type='string', unit=None)] comment=None data=None grants=None name='brz_stock_prices' primary_key=None schema_name='finance' timestamp_key='data.created_at' builder=TableBuilder(drop_source_columns=True, drop_duplicates=None, event_source=None, joins=[], pipeline_name=None, table_source=TableDataSource(read_as_stream=True, catalog_name='prod', cdc=None, selects=None, filter=None, from_pipeline=True, name='brz_stock_prices', schema_name='finance', watermark=None), layer='SILVER')
```

To get started with a more useful example, jump into the [Quickstart](https://www.laktory.ai/quickstart/).



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

