??? "API Documentation"
    [`laktory.api.register_dataframe_namespace`][laktory.api.register_dataframe_namespace]<br>
    [`laktory.api.register_lazyframe_namespace`][laktory.api.register_lazyframe_namespace]<br>
    [`laktory.api.register_anyframe_namespace`][laktory.api.register_anyframe_namespace]<br>
    [`laktory.api.register_expr_namespace`][laktory.api.register_expr_namespace]<br>
    [`laktory.api.register_spark_dataframe_namespace`][laktory.api.register_spark_dataframe_namespace]<br>
    [`laktory.api.register_spark_column_namespace`][laktory.api.register_spark_column_namespace]<br>

While [Narwhals](https://narwhals-dev.github.io/narwhals/) and [Laktory](extension_laktory.md) provide a rich set of
built-in DataFrame methods, Laktory also supports the creation of **custom namespaces** for registering your own
methods and functions, callable from pipeline YAML.

## Narwhals namespaces

Use Narwhals namespaces when you want your custom logic to work across all backends and
plan on manipulating a Narwhals DataFrame.
Methods are written against the Narwhals API and are backend-agnostic.

```py
import narwhals as nw
import polars as pl

import laktory as lk


@lk.api.register_anyframe_namespace("custom")
class CustomNamespace:
    def __init__(self, _df):
        self._df = _df  # Narwhals DataFrame

    def with_x2(self):
        return self._df.with_columns(x2=nw.col("x") * 2)


df = nw.from_native(pl.DataFrame({"x": [0, 1]}))

df = df.custom.with_x2()
```

Use `register_dataframe_namespace` or `register_lazyframe_namespace` to restrict the registration
to a specific frame type, and `register_expr_namespace` to extend column expressions.

### In a pipeline

```yaml
name: my_pipeline
nodes:
  - name: slv_prices
    source: ...
    transformer:
      nodes:
        - func_name: custom.with_x2
          dataframe_api: NARWHALS
```

## Spark namespaces

Use Spark namespaces when your team works exclusively with PySpark and you want to write pure Spark
code — no Narwhals imports required.

```py
import pyspark.sql.functions as F
import laktory as lk


@lk.api.register_spark_dataframe_namespace("custom")
class CustomOps:
    def __init__(self, _df):
        self._df = _df  # native PySpark DataFrame

    def with_x2(self):
        return self._df.withColumn("x2", F.col("x1") * 2)
```

### In a pipeline

```yaml
name: my_pipeline
nodes:
  - name: slv_prices
    source: ...
    transformer:
      nodes:
        - func_name: custom.with_x2
          dataframe_api: NATIVE
```

### Spark column namespaces

Use `register_spark_column_namespace` to extend PySpark `Column` objects with reusable
expression helpers. These are accessible from `func_args` strings when `DATAFRAME_API=NATIVE`.

```py
import laktory as lk


@lk.api.register_spark_column_namespace("custom")
class CustomColOps:
    def __init__(self, _col):
        self._col = _col

    def double(self):
        return self._col * 2
```

```yaml
transformer:
  nodes:
    - func_name: withColumn
      func_args:
        - x2
        - "col('x1').custom.double()"
      dataframe_api: NATIVE
```

## Packaging custom extensions

You can combine custom namespaces with a [Python Package](extension_python_package.md) to bundle
and distribute your extensions, making them available to any Laktory pipeline that lists the
package as a dependency.
