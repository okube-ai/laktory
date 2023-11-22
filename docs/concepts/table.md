??? "API Documentation"
    [`laktory.models.sql.table`](TODO)<br>

The `Table` model is a fundamental component when building a Lakehouse.
In the context of Laktory, it can serve a few purposes

## SQL Object
The `Table` model may be used to declare a classic SQL object
```py
from laktory import models
table = models.Table(
    schema_name="finance",
    catalog_name="dev",
    name="slv_stock_prices",
    comment="Stock Prices",
    columns=[
        {"name": "symbol", "type": "string"},
        {"name": "open", "type": "double"},
        {"name": "close", "type": "double"},
    ]
)
```
This will be generally sufficient to create the associated object within a schema. 

## Data
One could potentially even specify the data such that the resulting table is also populated with rows.
```py
table.data = [
    ["AAPL", 190.50, 192.30],
    ["GOOGL", 136.25, 137.43],
]

df = table.to_df()
```

## Builder
However, where things get very interesting is when the builder is used.
```py
from laktory import models
table = models.Table(
    schema_name="finance",
    catalog_name="dev",
    name="slv_stock_prices",
    comment="Stock Prices",
    columns=[
        {"name": "symbol", "type": "string", "sql_expression": "data.symbol"},
        {"name": "open", "type": "double", "spark_func_name": "coalesce", "spark_func_args": ["daa.open"]},
        {"name": "close", "type": "double", "spark_func_name": "coalesce", "spark_func_args": ["daa.close"]},
    ],
    builder={
        "zone": "SILVER",
        "table_source": {
            "name": "brz_stock_prices",
        },
    }
)
```
Given this definition, the builder can be used to read the data source and sets the newly declared columns.
Note that each column may be defined as an SQL expression or as the output of a spark function.
The builder `zone` sets predefined settings like dropping source columns or duplicated rows.
```py
# Read source data ("brz_stock_prices")
df = table.builder.read_source(spark)

# Build output columns (`symbol`, `open` and `close`)
df = table.builder.process(df=df, spark=spark)
```
The builder also supports more advanced features like sequential joins or CDC definition.

The most frequent use case for the declaration of a `Table` and its associated transformation is as part of a data [pipeline](pipeline.md).