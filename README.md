
# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![test](https://github.com/okube-ai/laktory/actions/workflows/test.yml/badge.svg)](https://github.com/okube-ai/laktory/actions/workflows/test.yml)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

An open-source, dataframe-centric ETL framework for building lakehouses with a 
DataOps approach.

<img src="docs/images/logo_sg.png" alt="laktory logo" width="85"/>

Laktory, the lakehouse factory, is an open-source framework designed for 
building, deploying, and executing data pipelines while adhering to essential
DevOps best practices such as source control, code reviews, and CI/CD. 
Leveraging Apache Spark and Polars as its core data transformation engines,
Laktory ensures robust and scalable data processing capabilities.

<img src="docs/images/laktory_diagram.png" alt="what is laktory" width="400"/>

While a Laktory data pipeline can be run locally for small datasets or 
prototyping, it really starts to shine when deployed and orchestrated on a 
cloud data platform, such as Databricks. When combined with [Delta Live Tables](https://www.databricks.com/product/delta-live-tables),
it provides a top-tier, simple and low maintenance fully managed solution.  

Beyond just data pipelines, Laktory allows for the comprehensive definition
and deployment of your entire data platform. This includes everything from 
cloud infrastructure to data tables, security, and quality monitoring systems,
providing an all-in-one solution for modern data platform management.

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

node_brz = models.PipelineNode(
    name="brz_stock_prices",
    layer="BRONZE",
    source={
        "format": "PARQUET",
        "path": "./data/brz_stock_prices/"
    },
    transformer={
        "nodes": [
            {
                "func_name": "select",
                "func_args": [
                    "symbol",
                    "timestamp",
                    "open",
                    "close",
                ],
            },
        ]
    }
)

node_slv = models.PipelineNode(
    name="slv_stock_prices",
    layer="SILVER",
    source={
        "node_name": "brz_stock_prices"
    },
    sink={
        "path": "./data/slv_stock_prices",
        "mode": "OVERWRITE",
        "format": "PARQUET",
    },
    transformer={
        "nodes": [
            {
                "func_name": "drop_duplicates",
                "func_kwargs": {
                    "subset": ["timestamp", "symbol"]
                }
            },
        ]
    }
)

pipeline = models.Pipeline(
    name="stock_prices",
    nodes=[node_brz, node_slv],
)

print(pipeline)
#> resource_name_=None options=ResourceOptions(variables={}, depends_on=[], provider=None, aliases=None, delete_before_replace=True, ignore_changes=None, import_=None, parent=None, replace_on_changes=None) variables={} databricks_job=None dlt=None name='stock_prices' nodes=[PipelineNode(variables={}, add_layer_columns=True, dlt_template='DEFAULT', description=None, drop_duplicates=None, drop_source_columns=False, transformer=SparkChain(variables={}, nodes=[SparkChainNode(variables={}, allow_missing_column_args=False, column=None, spark_func_args=[SparkFuncArg(variables={}, value='symbol'), SparkFuncArg(variables={}, value='timestamp'), SparkFuncArg(variables={}, value='open'), SparkFuncArg(variables={}, value='close')], spark_func_kwargs={}, spark_func_name='select', sql_expression=None)]), expectations=[], layer='BRONZE', name='brz_stock_prices', primary_key=None, sink=None, source=FileDataSource(variables={}, as_stream=False, broadcast=False, cdc=None, dataframe_type='SPARK', drops=None, filter=None, mock_df=None, renames=None, selects=None, watermark=None, format='PARQUET', header=True, multiline=False, path='./data/brz_stock_prices/', read_options={}, schema_location=None), timestamp_key=None), PipelineNode(variables={}, add_layer_columns=True, dlt_template='DEFAULT', description=None, drop_duplicates=None, drop_source_columns=True, transformer=SparkChain(variables={}, nodes=[SparkChainNode(variables={}, allow_missing_column_args=False, column=None, spark_func_args=[], spark_func_kwargs={'subset': SparkFuncArg(variables={}, value=['timestamp', 'symbol'])}, spark_func_name='drop_duplicates', sql_expression=None)]), expectations=[], layer='SILVER', name='slv_stock_prices', primary_key=None, sink=FileDataSink(variables={}, mode='OVERWRITE', checkpoint_location=None, format='PARQUET', path='./data/slv_stock_prices', write_options={}), source=PipelineNodeDataSource(variables={}, as_stream=False, broadcast=False, cdc=None, dataframe_type='SPARK', drops=None, filter=None, mock_df=None, renames=None, selects=None, watermark=None, node_name='brz_stock_prices', node=PipelineNode(variables={}, add_layer_columns=True, dlt_template='DEFAULT', description=None, drop_duplicates=None, drop_source_columns=False, transformer=SparkChain(variables={}, nodes=[SparkChainNode(variables={}, allow_missing_column_args=False, column=None, spark_func_args=[SparkFuncArg(variables={}, value='symbol'), SparkFuncArg(variables={}, value='timestamp'), SparkFuncArg(variables={}, value='open'), SparkFuncArg(variables={}, value='close')], spark_func_kwargs={}, spark_func_name='select', sql_expression=None)]), expectations=[], layer='BRONZE', name='brz_stock_prices', primary_key=None, sink=None, source=FileDataSource(variables={}, as_stream=False, broadcast=False, cdc=None, dataframe_type='SPARK', drops=None, filter=None, mock_df=None, renames=None, selects=None, watermark=None, format='PARQUET', header=True, multiline=False, path='./data/brz_stock_prices/', read_options={}, schema_location=None), timestamp_key=None)), timestamp_key=None)] orchestrator=None udfs=[]

pipeline.execute(spark=spark)
```

To get started with a more useful example, jump into the [Quickstart](https://www.laktory.ai/quickstart/).


## A Lakehouse DataOps Template
A comprehensive template on how to deploy a lakehouse as code using Laktory is maintained here:
https://github.com/okube-ai/lakehouse-as-code.

In this template, 4 pulumi projects are used to:
- `{cloud_provider}_infra`: Deploy the required resources on your cloud provider
- `unity-catalog`: Setup users, groups, catalogs, schemas and Lakehouse grant
- `workspace-conf`: Setup secrets, clusters and warehouses
- `workspace`: The data workflows to build your lakehouse.

## Okube Company
<img src="docs/images/okube.png" alt="okube logo" width="85"/>

[Okube](https://www.okube.ai) is dedicated to building open source frameworks, known as the *kubes*, empowering businesses to build, deploy and operate highly scalable data platforms and AI models. 

