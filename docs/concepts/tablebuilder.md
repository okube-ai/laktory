??? "API Documentation"
    [`laktory.models.resources.databricks.TableBuilder`][laktory.models.resources.databricks.TableBuilder]<br>

The `TableBuilder` model is a core component of Laktory that provides the mechanisms for building highly complex tables from simple configuration.
It supports data input, columns creation, joins and aggregations. 

### Layers
The table builder lets you input a target layer (`BRONZE`, `SILVER`, `GOLD`) that define some custom options. 
For example the `SILVER` tables will drop source columns and duplicates by default.
The layer also sets the default notebook template.
Each of these default may also be overwritten, regardless of the `layer` value.


### Data Sources
The `DataSource` models facilitate loading data into a spark DataFrame. 
It provides re-usable mechanisms for reading data of various nature given different configuration.

#### File Data Source
??? "API Documentation"
    [`laktory.models.FileDataSource`][laktory.models.FileDataSource]<br>

This type of data source supports reading multiple files stored on a storage container

```py
from laktory import models

source = models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="JSON",
    as_stream=False,
)
df = source.read()
```

Reading the same dataset, but as a spark streaming source, is as easy as changing `read_as_stream` to `True`.
```py
from laktory import models

source = models.FileDataSource(
    path="/Volumes/sources/landing/events/yahoo-finance/stock_price",
    format="JSON",
    as_stream=True,
)
df_stream = source.read()
```

#### Table Data Source
??? "API Documentation"
    [`laktory.models.TableDataSource`][laktory.models.TableDataSource]<br>
When your data is already loaded into a table, you can use the `TableDataSource` model instead

```py
from laktory import models

source = models.FileDataSource(
    table_name="brz_stock_prices",
    selects=["symbol", "open", "close"],
    filter="symbol='AAPL'",
    from_dlt=False,
    as_stream=True,
)
df = source.read()
```
In this case

* the `selects` argument is used to select only `symbol`, `open` and `close` columns
* the `filter` argument is used to select only rows associated with Apple stock. 
* the `from_dlt` argument can be used in the context of a data [pipeline](pipeline.md) to reference another table part of the same pipeline.  

More data sources (like Kafka / Event Hub / Kinesis streams) will be supported in the future.


### Spark Chain
??? "API Documentation"
    [`laktory.models.SparkChain`][laktory.models.SparkChain]<br>

Spark Chain is the core model allowing to transform your source data into the desired state 
for the output table. It is essentially a serialization of chained spark 
operations with support for data sources in addition to DataFrames.

A Spark Chain is defined as a series of nodes, each one representing a 
transformation applied to a DataFrame. A node declare the spark function
responsible for the transformation and the arguments to pass to that function.
Each function is expected to output a DataFrame and receive as an input the 
output of the previous node. As a convenience, a node can also declare a new
column. In this case, the function is expected to output a column.

For example, consider a simple DataFrame with column `x` for which you want to:

- rename `x` to `theta`
- compute `cos(theta)`
- drop duplicated rows

here is how you would do it with `SparkChain`
```python
import pandas as pd
from laktory import models

df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))

# Build Chain
sc = models.SparkChain(
    nodes=[
        {
            "spark_func_name": "withColumnRenamed",
            "spark_func_args": ["x", "theta"],
        },
        {
            "column": {
                "name": "cos",
                "type": "double",
            },
            "spark_func_name": "cos",
            "spark_func_args": ["theta"],
        },
        {
            "spark_func_name": "drop",
            "spark_func_args": ["x_tmp"],
        },
    ]
)

# Execute Chain
df = sc.execute(df0, spark=spark)
```

### Process
The `TableBuilder.process` method executes the spark chain described above and
other default laktory spark chains to the input dataframe.


### Putting it all together
Here is a complete example of table builder declaration.
```python
from laktory import models

table = models.Table(
    name="slv_stock_prices",
    builder={
        "layer": "SILVER",
        "source": {
            "table_name": "brz_stock_prices",
        },
        "spark_chain": {
            "nodes": [
                {
                    "column": {"name": "symbol", "type": "string"},
                    "sql_expression": "data.symbol",
                },
                {
                    "column": {
                        "name": "open",
                        "type": "double",
                    },
                    "spark_func_name": "coalesce",
                    "spark_func_args": ["daa.open"],
                },
                {
                    "column": {
                        "name": "close",
                        "type": "double",
                    },
                    "spark_func_name": "coalesce",
                    "spark_func_args": ["daa.close"],
                },
            ]
        },
    },
)

# Read
df = table.builder.read_source(spark=spark)

# Process
df = table.builder.process(df, spark=spark)
```
