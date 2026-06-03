??? "API Documentation"
    [`laktory.models.DataFrameTransformer`][laktory.models.DataFrameTransformer]<br>

<img src="/../../images/diagrams/transformer_logo.png" alt="node transformer" width="100"/>

The `DataFrameTransformer` class is the core model for transforming dataframes in Laktory. It serializes chained 
DataFrame operations and is generally used between a data source and a data sink.

<img src="/../../images/diagrams/transformer_diagram.png" alt="node transformer" width="600"/>

A Transformer is composed of a series of nodes, each representing a transformation applied to a dataframe as an 
expression or as a method. The output of one node is passed as input to the next, enabling complex and modular
transformations.

By default, operations are declared using [Narwhals DataFrames](https://narwhals-dev.github.io/narwhals/basics/dataframe/) 
API, but it can also be configured to use the selected DataFrame backend API. 

#### DataFrameExpr
??? "API Documentation"
    [`laktory.models.DataFrameExpr`][laktory.models.DataFrameExpr]<br>

The `DataFrameExpr` class expresses a transformation as a `SELECT` SQL expression, including joins and unions.

#### DataFrameMethod
??? "API Documentation"
    [`laktory.models.DataFrameMethod`][laktory.models.DataFrameMethod]<br>

The `DataFrameMethod` class expresses a transformation as a method applied to the DataFrame and its required arguments.
By default, Narwhals DataFrame API is used, but native DataFrame backend can also be selected. You can also extend 
this functionality by creating a custom [namespace](extension_custom.md).

For example, if you have a dataframe with a column `x` and want to:

- rename `x` to `theta` using SQL
- compute `cos(theta)` using Narwhals API
- drop duplicated rows using Spark

here is how you would do it:

=== "YAML"
    ```yaml
    nodes:
    - expr: SELECT x AS theta FROM {df}
    - func_name: with_columns
      func_kwargs:
         cos: nw.col('theta').cos()
    - func_name: drop_duplicates
      func_args: theta
      dataframe_api: NATIVE
    ```

=== "Python"
    ```python
    import pandas as pd
    
    import laktory as lk
    
    df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))
    
    # Build Chain
    sc = lk.models.DataFrameTransformer(
        nodes=[
            {
                "expr": "SELECT x AS theta FROM {df}",
            },
            {
                "func_name": "with_columns",
                "func_kwargs": {
                    "cos": "nw.col('theta').cos()"
                }
            },
            {
                "func_name": "drop_duplicates",
                "func_args": ["theta"],
                "dataframe_api": "NATIVE",
            },
        ]
    )
    
    # Execute Chain
    df = sc.execute(df0)
    ```

For a more modular, scalable, and testable solution, we recommend using Spark over SQL. You can learn more in the blog 
posts
[Spark vs SQL](https://www.linkedin.com/pulse/sparkling-queries-in-depth-spark-vs-sql-data-pipelines-olivier-soucy-nfyve/)
and
[SparkChain](https://www.linkedin.com/pulse/laktory-sparkchain-serializable-spark-based-data-olivier-soucy-oihxe/)
.

## DataFrame References

Both `DataFrameExpr` (SQL) and `DataFrameMethod` (API) transformer nodes support a set of `{placeholder}` references
to identify which DataFrame to read from or pass as an argument. These are **not** model [variables](variables.md) —
they are resolved at execution time by the transformer engine and only work inside transformer expressions and method
arguments.

### `{df}` — the flowing DataFrame

`{df}` always refers to the DataFrame currently flowing through the transformer chain:

- **First transformer node** — `{df}` is the primary source (the first entry in the node's `sources` list).
- **Subsequent nodes** — `{df}` is the output of the previous step.

```yaml
transformer:
  nodes:
  - expr: SELECT symbol, open, close FROM {df}   # {df} = primary source
  - expr: SELECT * FROM {df} WHERE open > 100    # {df} = output of previous step
  - func_name: with_columns
    func_kwargs:
      spread: nw.col('high') - nw.col('low')     # no {df} needed — method acts on flowing df
```

### `{sources.name}` — a named pipeline node source

When a pipeline node declares multiple [sources](sourcessinks.md), each source can be assigned a `name`. That name
can then be used as a placeholder in both SQL expressions and method arguments:

```yaml
nodes:
- name: slv_stocks
  sources:
  - name: prices
    node_name: brz_stock_prices
  - name: metadata
    node_name: brz_stock_metadata
    selects: [symbol, currency]
  transformer:
    nodes:
    # SQL join using both named sources
    - expr: |
        SELECT p.symbol, p.open, p.close, m.currency
        FROM {sources.prices} p
        LEFT JOIN {sources.metadata} m ON p.symbol = m.symbol

    # Equivalent using a method argument
    - func_name: join
      func_kwargs:
        other: "{sources.metadata}"
        on: symbol
        how: left
```

When a node has a single source, a `name` is not required — use `{df}` instead.

### `{nodes.node_name}` — an upstream pipeline node's output

Any upstream pipeline node's output DataFrame can be referenced directly by name, without declaring it as a source
on the current node. This is useful for ad-hoc lookups or unions across branches of the pipeline DAG:

```yaml
transformer:
  nodes:
  - expr: |
      SELECT * FROM {df}
      UNION ALL
      SELECT * FROM {nodes.node_01}
      UNION ALL
      SELECT * FROM {nodes.node_02}
```
