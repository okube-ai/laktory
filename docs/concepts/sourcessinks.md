## Data Sources
<img src="/../../images/diagrams/source_logo.png" alt="node source" width="100"/>

The `DataSource` models facilitate loading data into a dataframe. It provides 
reusable mechanisms for reading data of various nature given different
configuration and various execution contexts.

It is generally used as a component of a [pipeline](pipeline.md) node. In this
context, the sink may be used to store the output of the node or some
quarantined data if [expectations](dataquality.md) are set and not met.

#### Naming Sources
A pipeline node can declare one or more sources via its `sources` list. Each source accepts an optional `name` field.
The name is used to reference that source inside [transformer](transformer.md) expressions using the
`{sources.name}` placeholder:

```yaml
nodes:
- name: slv_stocks
  sources:
  - name: prices        # referenced as {sources.prices}
    node_name: brz_stock_prices
  - name: metadata      # referenced as {sources.metadata}
    node_name: brz_stock_metadata
    selects: [symbol, currency]
  transformer:
    nodes:
    - func_name: join
      func_kwargs:
        other: "{sources.metadata}"
        on: symbol
```

When a node has only one source, `name` is optional and the source is accessible as `{df}` in transformer
expressions (which always refers to the flowing DataFrame - the primary source for the first step, and the output
of the previous step for subsequent ones).

```yaml
nodes:
- name: slv_stock_prices
  sources:
  - node_name: brz_stock_prices   # no name needed for a single source
  transformer:
    nodes:
    - expr: SELECT symbol, open, close FROM {df}
```


#### File Data Source
??? "API Documentation"
    [`laktory.models.FileDataSource`][laktory.models.FileDataSource]<br>

File data source supports reading files stored on disk.
```py
import laktory as lk

source = lk.models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_prices/",
    format="JSON",
    as_stream=False,
    dataframe_backend="PYSPARK"
)
df = source.read()
```
Reading the same dataset, but as a spark streaming source, is as easy as changing 
`as_stream` to `True`.

```py
from laktory import models

source = models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="JSON",
    as_stream=True,
    dataframe_backend="PYSPARK"
)
df_stream = source.read()
```

You can also select a different DataFrame backend for reading your files
```py
import laktory as lk

source = lk.models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_prices.parquet",
    format="PARQUET",
    dataframe_backend="POLARS"
)
df = source.read()
```

#### Table Data Source
??? "API Documentation"
    [`laktory.models.HiveMetastoreDataSource`][laktory.models.HiveMetastoreDataSource]<br>
    [`laktory.models.UnityCatalogDataSource`][laktory.models.UnityCatalogDataSource]<br>

When your data is already loaded into data table, you can use the 
`UnityCatalogDataSource` or `HiveMetastoreDataSource` models instead

```py
import laktory as lk

source = lk.models.UnityCatalogDataSource(
    table_name="brz_stock_prices",
    selects=["symbol", "open", "close"],
    filter="symbol='AAPL'",
    as_stream=True,
)
df = source.read()
```
In this case

* the `selects` argument is used to select only `symbol`, `open` and `close` columns
* the `filter` argument is used to select only rows associated with Apple stock.  

More data sources (like Kafka / Event Hub / Kinesis streams) will be supported
in the future.

#### Pipeline Node Data Source
??? "API Documentation"
    [`laktory.models.PipelineNodeDataSource`][laktory.models.PipelineNodeDataSource]<br>

To establish a relationship between two nodes in a data pipeline, the 
`PipelineNodeDataSource` must be used. Assuming each node is a vertex in a 
directed acyclic graph (DAG), using a `PipelineNodeDataSource` creates an edge
between two vertices. It also defines the execution order of the nodes.
```py
import laktory as lk

source = lk.models.PipelineNodeDataSource(
    node_name="brz_stock_prices",
    as_stream=True,
)
```
This type of data source adapts to its execution context.

* Single Worker Execution: The source uses the in-memory output dataframe from
  the upstream node.
* Multi-Workers Execution: The source uses the upstream node sink as a source 
  for read the dataframe.
* Declarative Pipeline Execution: The source uses `spark.read()` and `spark.readStream()` to read 
  data from the upstream node.
     

## Data Sinks
??? "API Documentation"
    [`laktory.models.FileDataSink`][laktory.models.FileDataSink]<br>
    [`laktory.models.UnityCatalogDataSink`][laktory.models.UnityCatalogDataSink]<br>

<img src="/../../images/diagrams/sink_logo.png" alt="node sink" width="100"/>

Analogously to `DataSource`, `DataSink` models facilitate writing a dataframe
into a target location. It provides re-usable mechanisms for writing data 
in various formats, adapting to different execution contexts.

It is generally used as a component of a [pipeline](pipeline.md) node.

Data sinks also support the merge of a [Change Data Capture (CDC)](cdc.md).

#### File Data Sink
??? "API Documentation"
    [`laktory.models.FileDataSink`][laktory.models.FileDataSink]<br>

File data sink supports writing a dataframe as files to a disk location
using a variety of storage format. For streaming dataframes, you also need to
specify a checkpoint location.

```py
import narwhals as nw
import polars as pl

import laktory as lk

df = nw.from_native(
    pl.DataFrame({"symbol": ["AAPL", "GOOGL"]})
)

sink = lk.models.FileDataSink(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price.parquet",
    format="PARQUET",
    mode="OVERWRITE",
)
sink.write(df)
```

#### Table Data Sink
??? "API Documentation"
    [`laktory.models.UnityCatalogDataSink`][laktory.models.UnityCatalogDataSink]<br>
    [`laktory.models.HiveMetastoreDataSink`][laktory.models.HiveMetastoreDataSink]<br>

The `UnityCatlaogDataSink` and `HiveMetastoreDataSink` classes provide a convenient way
to write a DataFrame to a data table. It simplifies the process of persisting data
in a structured format, supporting both physical tables and SQL views.

To write a DataFrame to a physical table:
```py
import narwhals as nw

import laktory as lk

df = nw.from_native(
    spark.createDataFrame([("AAPL"), ("GOOGL")], ["symbol"])
)

sink = lk.models.UnityCatalogDataSink(
    schema_name="finance",
    table_name="brz_stock_prices",
)
sink.write(df)
``` 

`UnityCatlaogDataSink` also supports creating non-materialized SQL views instead of 
physical tables. To write a DataFrame as a SQL view:
```py
import narwhas as nw

import laktory as lk

df = nw.from_native(
    spark.createDataFrame([("AAPL"), ("GOOGL")], ["symbol"])
)

sink = lk.models.TableDataSink(
    schema_name="finance",
    table_name="brz_stock_prices",
    table_type="VIEW",
    view_definition="SELECT * from {df}"
)
sink.write(df)
``` 

##### Purge Modes

When a pipeline node is run with `full_refresh=True`, each of its sinks is purged before being
rewritten. `purge_mode` controls how that purge is done:

- `purge_mode="DROP"` (default): drops the table entirely. It's recreated (schema and all) the
  next time the sink is written to.
- `purge_mode="TRUNCATE"`: empties the table - removes all rows, via an unconditional
  `DELETE FROM` since Delta does not support the `TRUNCATE TABLE` SQL statement - but keeps the
  table, its schema, and its location intact.
- `purge_mode="DELETE_WHERE"`: deletes only the rows matching a `purge_delete_where` SQL
  predicate, leaving every other row untouched. This is a good fit when a table is written to by
  multiple, independently deployed pipelines (e.g. one pipeline per client appending into a
  shared, cross-tenant table) - scoping the predicate to the rows a given pipeline owns lets it
  reprocess its own data on `full_refresh` without touching what other pipelines wrote.
- `purge_mode="NONE"`: leaves the data untouched. The sink checkpoint is still deleted, so the
  node reprocesses and re-appends its data. See [Shared sinks](#shared-sinks) below.

```py
import laktory as lk

sink = lk.models.UnityCatalogDataSink(
    schema_name="finance",
    table_name="brz_stock_prices",
    purge_mode="DELETE_WHERE",
    purge_delete_where="client_id = 'acme'",
)
```

`purge_delete_where` requires DELTA format and must be set directly on the sink that owns the
predicate - it is not inherited from a parent pipeline node, pipeline, or global setting, since a
deletion predicate is inherently specific to one sink. `purge_mode="DELETE_WHERE"` follows the
same rule: it can only be set directly on a sink, and raises a validation error if set on a
`PipelineNode`, `Pipeline`, or globally (`settings.purge_mode` / `LAKTORY_PURGE_MODE`). `DROP` and
`TRUNCATE`, on the other hand, can be set at the sink, pipeline node, or pipeline level, or
globally via the `LAKTORY_PURGE_MODE` environment variable / `settings.purge_mode` (see
[Laktory Settings](laktorysettings.md)).

Because a wrong or stale `purge_delete_where` predicate could otherwise silently delete the wrong
rows, Laktory logs the number of rows matched by the predicate immediately before deleting them.

`TRUNCATE`/`DELETE_WHERE` are only supported for table sinks today; a `FileDataSink` only
supports `purge_mode="DROP"` and `purge_mode="NONE"`.

##### Shared sinks

Multiple nodes of a pipeline can write to the same table or path, for example several `APPEND`
feeds pooled into one table. With the default `DROP`, a `full_refresh` would make each node drop
the data just written by the others. Instead, let a single node drive the purge, set
`purge_mode: NONE` on the other nodes, and execute them after it using `depends_on`:

```yaml
nodes:
  - name: feed_a
    sinks:
      - table_name: pooled
        mode: APPEND
  - name: feed_b
    depends_on: [feed_a]
    purge_mode: NONE
    sinks:
      - table_name: pooled
        mode: APPEND
```

On `full_refresh`, `feed_a` drops `pooled` and `feed_b` only resets its checkpoint, so both
reprocess their data into the new table. Laktory raises a warning when a pipeline is validated with
more than one node writing to the same sink with `DROP` or `TRUNCATE`, and an error if such a
node is executed with `full_refresh`. Nodes using `DELETE_WHERE` each delete their own rows and
don't need `NONE`.

Writers of a shared sink may run in parallel - concurrent Delta appends don't conflict - but on
`full_refresh`, the purging node could then delete rows already written by the others. Laktory
raises a validation warning for any writer that isn't executed after the purging node. Parallel
writers also conflict when they change the table schema (e.g. appending different columns with
schema merging); declaring the full table `schema` on the sinks avoids it.

Executing a `NONE` node alone with `full_refresh` doesn't purge the shared data, so its rows are
appended again. Use `DELETE_WHERE` (with a column identifying each node's rows) to refresh a
single writer in isolation.

With Lakeflow / Spark Declarative Pipeline orchestrators, the shared table is declared once as a
streaming table and each node appends to it through its own append flow, named
`{table_name}__{node_name}`. The declarative engine runs the flows in parallel and handles
`full_refresh` itself - clearing the table once and resetting every flow - so neither
`purge_mode: NONE` nor `depends_on` is needed. The following rules apply:

- All sinks must be streaming, non-CDC (`MERGE`) table sinks.
- Table properties (`comment`, `table_properties`, `format`) can be declared on any of the sinks,
  but must not conflict.
- With Lakeflow Declarative Pipelines, expectations are applied to the whole table (append flows
  don't support them), so all nodes writing to it must declare the same expectations.

A flow checkpoint is identified by its name. Adding a second node to an existing single-writer
streaming table (or renaming a node) changes the flow name of the existing writer, which then
reprocesses its source from scratch - run a full refresh of that table once after the change.

#### Pipeline View Data Sink
??? "API Documentation"
    [`laktory.models.PipelineViewDataSink`][laktory.models.PipelineViewDataSink]<br>
    [`laktory.models.PipelineViewDataSink`][laktory.models.PipelineViewDataSink]<br>

The `PipelineViewDataSink` can be used in the context of a [Declarative Pipeline](https://www.databricks.com/blog/bringing-declarative-pipelines-apache-spark-open-source-project) 
such as Databricks Lakeflow Declarative Pipeline. A virtual view is created in the context of the pipeline, but the
data is not materialized. Views are useful for simplifying complex queries, encapsulating business logic, and providing
a consistent interface to the underlying data without duplicating storage.

```py
import laktory as lk

sink = lk.models.PipelineViewDataSink(
    pipeline_view_name="brz_stock_prices",
)
```