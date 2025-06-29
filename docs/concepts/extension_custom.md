??? "API Documentation"
    [`laktory.api.register_dataframe_namespace`][laktory.api.register_dataframe_namespace]<br>
    [`laktory.api.register_expr_namespace`][laktory.api.register_expr_namespace]<br>
    [`laktory.api.register_lazyframe_namespace`][laktory.api.register_lazyframe_namespace]<br>

While [Narwhals](https://narwhals-dev.github.io/narwhals/) and [Laktory](extension_laktory.md) provide a rich set of
built-in DataFrame methods, Laktory also supports the creation of **custom namespaces** for registering your own
methods and functions.  

Here’s an example of registering a custom DataFrame method:

```py
import narwhals as nw
import polars as pl

import laktory as lk


@lk.api.register_dataframe_namespace("custom")
class CustomNamespace:
    def __init__(self, _df):
        self._df = _df

    def with_x2(self):
        return self._df.with_columns(x2=nw.col("x") * 2)


df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

df = df.custom.with_x2()
```

Once registered, the method can be called in a pipeline node transformer.
```yaml
name: slv_stock_prices
source: ...
sinks: ...
transformer:
  nodes:
    - func_name: custom.with_x2
```

This same approach can be used to extend the functionality of Narwhals `Expr` and `LazyFrame` objects as well.

You can combine this feature and the development of a [Python Package](extension_python_package.md) 
to define custom functions available from a Laktory pipeline.