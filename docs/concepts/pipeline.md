??? "API Documentation"
    [`laktory.models.Pipeline`][laktory.models.Pipeline]<br>
    [`laktory.models.PipelineNode`][laktory.models.PipelineNode]<br>

The Pipeline model is the most fundamental component of Laktory. It declares 
how data is read, transformed and written.

A pipeline is defined as a collection of nodes, each producing a DataFrame by
reading a source, applying transformations, and optionally writing the output 
to a sink.

```yaml
name: stock_prices
nodes:
  - name: brz_stock_prices
    layer: BRONZE
    source:
      path: "./events/stock_prices"
    sink:
      schema_name: finance
      table_name: brz_stock_prices
    chain: 
      nodes: []
  - name: slv_stock_prices
    layer: SILVER
    source:
      node_name: brz_stock_prices
    sink:
      schema_name: finance
      table_name: brz_stock_prices
    chain:
        nodes:
          - spark_func_name: select
            spark_func_args:
              - timestamp
              - symbol
              - open
              - close
              - high
              - low
              - volume
          - spark_func_name: drop_duplicates
            spark_func_kwargs:
              subset:
                - symbol
                - timestamp
  ...
```

### Sources and Sinks
Various [sources and sinks](./sources.md) are supported, ranging from data 
files to data warehouse tables. By selecting a node as a source for another
downstream node, you establish the dependencies between different nodes, 
allowing you to build a directed acyclic graph (DAG).

### Spark Chain
Transformations are defined through a [chain](./sparkchain.md) of Spark 
(or Polars) function calls, offering a highly scalable, flexible, and 
customizable framework, particularly well-suited for streaming operations.

### Serialization
The entire pipeline definition is serializable, making it highly portable for
deployment on a remote compute positioning itself as an ideal candidate for a
DataOps approach using infrastructure as code.

## Layers
A pipeline node lets you selected the target medallion architecture layer
(`BRONZE`, `SILVER`, `GOLD`) so that basic transformations are automatically 
preset. For example the `SILVER` tables will drop source columns and duplicates 
by default.

## Orchestrators
The pipeline `pipeline.execute()` command makes it entirely possible to run
your data transformations locally or on a remote server. It will process each
node sequentially, reading data from the source, applying the transformations
and writing the sink if applicable. It supports streaming operations through
[Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
and various output formats and write modes through the configration of the 
sink.

However, selecting an orchestrator often enables more advanced features like
nodes parallel processing, automatic schema management and historical 
re-processing. Selecting and configuring the desired orchestrator is done directly in the
pipline configuration.

```yaml title="pipeline.yaml"
- name: stock_prices
  nodes: ...
  orchestrator: DLT
  dlt:
    catalog: dev
    target: finance
    configuration:
      pipeline_name: dlt-stock-prices
    
    clusters:
    - name : default
      node_type_id: Standard_DS3_v2
      autoscale:
        min_workers: 1
        max_workers: 2
    
    libraries:
    - notebook:
        path: /.laktory/dlt/dlt_laktory_pl.py
```

The choice of orchestrator will define which resources are deployed when 
running the `laktory deploy` CLI command. 

### Delta Live Tables (DLT)
[Databricks Delta Live Tables](https://www.databricks.com/product/delta-live-tables)
is the recommended orchestrator as it provides the most feature rich 
experience. Notably, it supports automatic schema changes management,
data quality checks, continuous execution, error handling, failure recovery
and autoscaling. 

![dlt](../images/dlt_stock_prices.png)

A Laktory pipeline integrates with DLT by executing each node inside a
`dlt.table()` or `dlt.view()` decorated function and returning the output
DataFrame. 

When used in the context of DLT, a node execution will not trigger a sink write
as this operation is managed by DLT. When a source is a pipeline node, 
`dlt.read()` and `dlt.read_stream()` functions will be called to ensure 
compatibility with the DLT framework. 


```py title="dlt_laktory_pl"
from laktory import dlt
from laktory import models

with open("pipeline.yaml") as fp:
    pl = models.Pipeline.model_validate_yaml(fp.read())


def define_table(node):
    @dlt.table_or_view(
        name=node.name,
        comment=node.description,
        as_view=node.sink is None,
    )
    @dlt.expect_all(node.warning_expectations)
    @dlt.expect_all_or_drop(node.drop_expectations)
    @dlt.expect_all_or_fail(node.fail_expectations)
    def get_df():

        # Execute node
        df = node.execute(spark=spark)

        # Return
        return df

    return get_df


# Build nodes
for node in pl.nodes:
    wrapper = define_table(node)
    df = dlt.get_df(wrapper)
    display(df)
```

Notice how `dlt` module is imported from laktory as it will provide additional
debugging and inspection capabilities. Notably, you can run the notebook in a
user cluster and will be able to inspect the resulting DataFrame.

![dlt](../images/dlt_debug.png)


### Databricks Job
A [Databricks Job](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
is another great orchestration mechanism. In this case, Laktory will create a 
task per node, allowing to parallelize nodes execution. Each reading and 
writing operation is entirely handled by Laktory source and sink 

![job](../images/job_stock_prices.png)

The supporting notebook simply needs to load the pipeline model and get the
node name from the job.

```py title="job_laktory_pl"
dbutils.widgets.text("pipeline_name", "pl-stock-prices")
dbutils.widgets.text("node_name", "")

from laktory import models
from laktory import settings

# --------------------------------------------------------------------------- #
# Read Pipeline                                                               #
# --------------------------------------------------------------------------- #

pl_name = dbutils.widgets.get("pipeline_name")
node_name = dbutils.widgets.get("node_name")
filepath = f"/Workspace{settings.workspace_laktory_root}pipelines/{pl_name}.json"
with open(filepath, "r") as fp:
    pl = models.Pipeline.model_validate_json(fp.read())


# --------------------------------------------------------------------------- #
# Execution                                                                   #
# --------------------------------------------------------------------------- #

if node_name:
    pl.nodes_dict[node_name].execute(spark=spark)
else:
    pl.execute(spark=spark)
```

### Apache Airflow
Supporting Apache Airflow as an orchestrator is currently under development and
will be release soon.

## Streaming
The event-based and kappa architectures promoted by Laktory lend themselves 
very well for Spark Structured Streaming, a real-time data processing framework
that enables continuous, scalable, and fault-tolerant processing of data 
streams. 

By setting `as_stream` to `True` in a pipeline node data source, the resulting
DataFrame will be streaming in nature and only new rows of data will be 
processed at each run instead of re-processing the entire data set.

Streaming does not mean that the pipeline is continuously running. Execution 
can still be scheduled, but each run is incremental. Selecting Delta Live
Tables orchestrator with `continuous: True` is currently the only way for
deploying a continuously running pipeline.

