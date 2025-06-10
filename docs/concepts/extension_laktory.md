[Narwhals](https://narwhals-dev.github.io/narwhals/) provides a rich set of DataFrame methods and expressions that work 
seamlessly across supported backends such as Spark and Polars. Laktory builds on top of Narwhals and introduces 
additional methods and functions under the `laktory` namespace.

## DataFrame
The DataFrame extension adds a `laktory` namespace to Narwhals `DataFrame` and `LazyFrame` objects. For example, you 
can use the [with_row_index](../api/narwhals_ext/dataframe/with_row_index.md) method:

```py
import narwhals as nw
import polars as pl

import laktory as lk  # noqa: F401

df0 = nw.from_native(
    pl.DataFrame(
        {
            "x": [0, 0, 0, 1, 1, 1],
            "y": [3, 2, 1, 1, 2, 3],
            "z": [0, 1, 3, 9, 16, 25],
        }
    )
)

df = df0.laktory.with_row_index(name="idx", order_by=["y"], partition_by="x")
```


## Expr
The expression extension adds a laktory namespace to Narwhals Expr objects. For example, you can use the 
[current_timestamp](../api/narwhals_ext/expr/convert_units.md) method:

```py
import narwhals as nw
import polars as pl

import laktory as lk  # noqa: F401

df = nw.from_native(pl.DataFrame({"x": [1]}))
df = df.with_columns(
    y=nw.col("x").laktory.convert_units(input_unit="m", output_unit="ft")
)
```

## Functions

The functions extension adds a laktory namespace to the Narwhals module. For example, you can use the 
[current_timestamp](../api/narwhals_ext/functions/current_timestamp.md) function:

```py
import narwhals as nw
import polars as pl

import laktory as lk  # noqa: F401

df = nw.from_native(pl.DataFrame({"x": [0, 1]}))
df = df.with_columns(tstamp=nw.laktory.current_timestamp())
```

You can also create your own [custom extension](extension_custom.md)