??? "API Documentation"
    [`laktory.models.Table`][laktory.models.Table]<br>

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
    ],
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
However, the most powerful table manipulation routines are provider in the [TableBuilder](tablebuilder.md) class.

The most frequent use case for the declaration of a `Table` and its associated transformations is as part of a data [pipeline](pipeline.md).