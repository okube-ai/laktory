## Data Sources
<img src="/../../images/diagrams/source_logo.png" alt="node source" width="100"/>

The `DataSource` models facilitate loading data into a dataframe. It provides 
reusable mechanisms for reading data of various nature given different
configuration and various execution contexts.

It is generally used as a component of a [pipeline](pipeline.md) node. In this
context, the sink may be used to store the output of the node or some
quarantined data if [expectations](dataquality.md) are set and not met.


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
* DLT Execution: The source uses `dlt.read()` and `dlt,read_stream()` to read 
  data from the upstream node.
     

## Data Sinks
??? "API Documentation"
    [`laktory.models.FileDataSink`][laktory.models.FileDataSink]<br>
    [`laktory.models.TableDataSink`][laktory.models.TableDataSink]<br>

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