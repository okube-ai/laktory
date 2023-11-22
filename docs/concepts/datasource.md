??? "API Documentation"
    [`laktory.models.datasources`](TODO)<br>

The `DataSource` models facilitate loading lakehouse data into a spark DataFrame. 
It provides re-usable mechanisms for reading data of various nature given different configuration.

### Event Data Source
This type of data source supports reading multiple files stored on a storage container
```py
from laktory import models
source = models.EventDataSource(
    name="stock_price",
    producer={"name": "yahoo-finance"},
    fmt="json",
    read_as_stream=False,
)
df = source.read()
```

Reading the same dataset, but as a spark streaming source, is as easy as changing `read_as_stream` to `True`.
```py
from laktory import models
source = models.EventDataSource(
    name="stock_price",
    producer={"name": "yahoo-finance"},
    fmt="json",
    read_as_stream=True,
)
df_stream = source.read()
```

### Table Data Source
When your data is already loading into a table, you can use the `TableDataSource` model instead
```py
from laktory import models
source = models.EventDataSource(
    name="brz_stock_prices",
    selects=["symbol", "open", "close"],
    filter="symbol='AAPL'",
    from_pipeline=False,
    read_as_stream=True,
)
df = source.read()
```
In this case

* the `selects` argument is used to select only `symbol`, `open` and `close` columns
* the `filter` argument is used to select only rows associated with Apple stock. 
* the `from_pipeline` argument can be used in the context of a data [pipline](pipeline.md) to reference another table part of the same pipeline.  

More data sources (like Kafka / Event Hub / Kinesis streams) will be supported in the future.