
# Laktory

[![pypi](https://img.shields.io/pypi/v/laktory.svg)](https://pypi.org/project/laktory/)
[![test](https://github.com/okube-ai/laktory/actions/workflows/test.yml/badge.svg)](https://github.com/okube-ai/laktory/actions/workflows/test.yml)
[![downloads](https://static.pepy.tech/badge/laktory/month)](https://pepy.tech/project/laktory)
[![versions](https://img.shields.io/pypi/pyversions/laktory.svg)](https://github.com/okube-ai/laktory)
[![license](https://img.shields.io/github/license/okube-ai/laktory.svg)](https://github.com/okube-ai/laktory/blob/main/LICENSE)

An open-source DataOps and dataframe-centric ETL framework for building 
lakehouses.

<img src="docs/images/logo_sg.png" alt="laktory logo" width="85"/>

Laktory is your all-in-one solution for defining both data transformations and 
Databricks resources. Imagine if Terraform, Databricks Asset Bundles, and dbt
combined forces—that’s essentially Laktory.

This open-source framework simplifies the creation, deployment, and execution 
of data pipelines while adhering to essential DevOps practices like version 
control, code reviews, and CI/CD integration. With Apache Spark and Polars
driving its data transformation, Laktory ensures reliable and scalable data
processing. Its modular, flexible approach allows you to seamlessly combine SQL
statements with DataFrame operations.

<img src="docs/images/laktory_diagram.png" alt="what is laktory" width="800"/>

Since Laktory pipelines are built on top of Spark and Polars, they can run in
any environment that supports python—from your local machine to a Kubernetes 
cluster. They can also be deployed and orchestrated as Databricks Jobs or
[Delta Live Tables](https://www.databricks.com/product/delta-live-tables),
offering a simple, fully managed, and low-maintenance solution.

But Laktory goes beyond data pipelines. It empowers you to define and deploy 
your entire Databricks data platform—from Unity Catalog and access grants
to compute and quality monitoring—providing a complete, modern solution
for data platform management. This empowers your data team to take full 
ownership of the solution, eliminating the need to juggle multiple technologies.
Say goodbye to relying on external Terraform experts to handle compute, workspace
configuration, and Unity Catalog, while your data engineers and analysts try 
to combine Databricks Asset Bundles and dbt to build data pipelines. Laktory
consolidates these functions, simplifying the entire process and reducing
the overall cost.

<img src="docs/images/why_simplicity.png" alt="dataops" width="500"/>


## Help
See [documentation](https://www.laktory.ai/) for more details.

## Installation
Install using 
```commandline
pip install laktory
```

For more installation options,
see the [Install](https://www.laktory.ai/install/) section in the documentation.

## A Basic Example
```py
from laktory import models


node_brz = models.PipelineNode(
    name="brz_stock_prices",
    source={
        "format": "PARQUET",
        "path": "./data/brz_stock_prices/"
    },
    transformer={
        "nodes": [
        ]
    }
)

node_slv = models.PipelineNode(
    name="slv_stock_prices",
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
            
            # SQL Transformation
            {
                "sql_expr": """
                    SELECT
                      data.created_at AS created_at,
                      data.symbol AS symbol,
                      data.open AS open,
                      data.close AS close,
                      data.high AS high,
                      data.low AS low,
                      data.volume AS volume
                    FROM
                      {df}
                """   
            },
            
            # Spark Transformation
            {
                "func_name": "drop_duplicates",
                "func_kwargs": {
                    "subset": ["created_at", "symbol"]
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
- `unity-catalog`: Setup users, groups, catalogs, schemas and manage grants
- `workspace`: Setup secrets, clusters and warehouses and common files/notebooks
- `workflows`: The data workflows to build your lakehouse

## Okube Company
<img src="docs/images/okube.png" alt="okube logo" width="85"/>

[Okube](https://www.okube.ai) is dedicated to building open source frameworks, known as the *kubes*, empowering businesses to build, deploy and operate highly scalable data platforms and AI models. 

