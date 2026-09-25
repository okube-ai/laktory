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
  table, its schema, its location and its grants intact.
- `purge_mode="DELETE_WHERE"`: deletes only the rows matching a `purge_delete_where` SQL
  predicate, leaving every other row untouched. For tables written by multiple pipelines, prefer
  [shared sinks](#shared-sinks), which track row ownership automatically.

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
supports `purge_mode="DROP"`.

The configured `purge_mode` can be overridden for a single run, e.g. to force a `DROP` after a
schema change on a sink configured with `TRUNCATE`:

- `pl.execute(full_refresh=True, purge_mode="DROP")`
- the `purge_mode` job parameter of the `LAKEFLOW_JOB` orchestrator (e.g. using *Run now with
  different parameters*, together with `full_refresh=true`)

##### Shared sinks

A sink can be written by multiple writers: other nodes of the same pipeline and/or other
pipelines (e.g. several feeds pooled into one table, or one pipeline per client appending into a
cross-tenant table). Declare it with the `shared` options, on every sink writing to the target:

```yaml
sinks:
- table_name: prices
  mode: APPEND
  shared:
    internal: true    # other nodes of this pipeline also write to this table
    external: false   # other pipelines also write to this table
    isolated: false   # true: own task, refreshes only its own rows
                      # false: grouped in a single task with the other writers
```

| `internal` | `external` | `isolated` | Execution | `full_refresh` |
|---|---|---|---|---|
| true | false | false | writers grouped in a single task | table purged once (`purge_mode`), then all writers reprocess |
| true | false | true | one task per writer (can run in parallel) | each node deletes and reprocesses its own rows |
| false | true | - | regular task | this pipeline's rows are deleted and reprocessed |
| true | true | false | writers grouped in a single task | this pipeline's rows are deleted once, then all writers reprocess |
| true | true | true | one task per writer | each node deletes and reprocesses its own rows |

**Grouped writers** (`internal`, not `isolated`) behave like a table with multiple append flows
in a declarative pipeline: the writers are executed together in one task - named
`shared-{table_name}`, or after their common `execution_task_name` - which purges the table once
and then runs them. Use `depends_on` to control their order, e.g. to have a node creating all the
columns run first. Selecting one of the writers (`selects`, or a task of a job run) always runs
all of them. Rows of a removed writer are gone after the next `full_refresh`.

**Isolated writers** (`isolated`) and **external writers** (`external`) require a DELTA table or
file sink. Each written row carries the writer identifier in a `_laktory_writer` column (first
column, configurable with `column`): `{pipeline_name}.{node_name}` for isolated writers,
`{pipeline_name}` otherwise (overridable with `writer_id`). On `full_refresh`, only the rows
of the writer are deleted, so the data of the other writers is untouched and `purge_mode`
can't be set. Keep the identifiers stable:

- Rows of a removed or renamed isolated node, or of a decommissioned pipeline, are not deleted by
  any `full_refresh`. Pin `writer_id` before renaming, or clean them up with
  `DELETE FROM <table> WHERE _laktory_writer = '<writer_id>'`.
- Rows written before a table is declared `external` or `isolated` have no writer: drop the table
  once when converting.
- Parallel writers adding different columns conflict when evolving the table schema: declare the
  full `schema` on the sinks, or group the writers.

To reset a whole `external` table (e.g. after a schema change), run a full refresh with the
`purge_mode` override (`DROP` or `TRUNCATE`): the table is purged once, including the rows written
by other pipelines, which then need a full refresh too. The override is not supported for
isolated writers - purge the table manually, then run a full refresh.

Laktory validates shared sinks: writers of a same target must all declare `internal` (and share
the same options), `internal` requires at least two writers in the pipeline, and pipelines of a
same Stack writing to the same target must all declare `external`.

With Lakeflow / Spark Declarative Pipeline orchestrators, only `internal` is supported: the table
is declared once as a streaming table and each node appends to it through its own append flow,
named `{table_name}__{node_name}`. The declarative engine runs the flows in parallel and handles
`full_refresh` itself, clearing the table once and resetting every flow. A table written by a
declarative pipeline can't be shared with other pipelines. The following rules apply:

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