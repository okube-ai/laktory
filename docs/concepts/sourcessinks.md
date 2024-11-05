## Data Sources
<img src="/../../images/source_logo.png" alt="node source" width="100"/>

The `DataSource` models facilitate loading data into a dataframe. It provides 
reusable mechanisms for reading data of various nature given different
configuration and various execution contexts.

It is generally used as a component of a [pipeline](pipeline.md) node. In this
context, the sink may be used to store the output of the node or some
quarantined data if [expectations](dataquality.md) are set and not met.


#### File Data Source
??? "API Documentation"
    [`laktory.models.FileDataSource`][laktory.models.FileDataSource]<br>

File data source supports reading multiple files stored on a storage container

```py
from laktory import models

source = models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="JSON",
    as_stream=False,
)
df = source.read(spark=spark)
```

Reading the same dataset, but as a spark streaming source, is as easy as changing `as_stream` to `True`.
```py
from laktory import models

source = models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="JSON",
    as_stream=True,
)
df_stream = source.read(spark=spark)
```

#### Table Data Source
??? "API Documentation"
    [`laktory.models.TableDataSource`][laktory.models.TableDataSource]<br>

When your data is already loaded into a lakehouse data table, you can use the 
`TableDataSource` model instead

```py
from laktory import models

source = models.TableDataSource(
    table_name="brz_stock_prices",
    selects=["symbol", "open", "close"],
    filter="symbol='AAPL'",
    warehouse="DATABRICKS",
    as_stream=True,
)
df = source.read(spark=spark)
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
from laktory import models

source = models.PipelineNodeDataSource(
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

<img src="/../../images/sink_logo.png" alt="node sink" width="100"/>

Analogously to `DataSource`, `DataSink` models facilitate writing a dataframe
into a target location. It provides re-usable mechanisms for writing data 
in various formats, adapting to different execution contexts.

It is generally used as a component of a [pipeline](pipeline.md) node.

#### File Data Sink
??? "API Documentation"
    [`laktory.models.FileDataSink`][laktory.models.FileDataSink]<br>

File data sink supports writing a dataframe as files to a disk location
using a variety of storage format. For streaming dataframes, you also need to
specify a checkpoint location.

```py
from laktory import models

sink = models.FileDataSink(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="PARQUET",
    mode="OVERWRITE",
)
sink.write(df)
```

#### Table Data Sink
??? "API Documentation"
    [`laktory.models.TableDataSink`][laktory.models.TableDataSink]<br>

The Table Data Sink allows writing the dataframe to a lakehouse or data 
warehouse table.

```py
from laktory import models

sink = models.FileDataSource(
    schema_name="finance",
    table_name="brz_stock_prices",
    warehouse="DATABRICKS",
)
sink.write(df)
``` 