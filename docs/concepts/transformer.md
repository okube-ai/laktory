<img src="/../../images/transformer_logo.png" alt="node transformer" width="100"/>

The transformer is the process in which data read from the source is modified before being written to a sink.

### Spark Chain
??? "API Documentation"
    [`laktory.models.SparkChain`][laktory.models.SparkChain]<br>

The Spark Chain is the core model for transforming dataframes in Laktory. It serializes chained Spark operations and 
supports [data sources](sourcessinks.md) instances as inputs in addition to dataframes.

<img src="/../../images/spark_chain_diagram.png" alt="node transformer" width="600"/>

A Spark Chain is a series of nodes, each representing a transformation applied to a dataframe. Each node can declare a
SQL transformation or a Spark function with the arguments passed to that function. The output of one node is passed
as input to the next, enabling complex transformations. Additionally, nodes can declare new columns derived from SQL
or Spark expressions.

A Spark chain is defined as a series of nodes, each one representing a transformation applied to a dataframe. A node
declares either a SQL transformation or the spark function responsible for the transformation and the arguments to
pass to that function. Each function is expected to output a dataframe and receive as an input the output of the 
previous node. As a convenience, a node can also declare a new column from a sql or spark expression.

For example, if you have a dataframe with a column `x` and want to:

- rename `x` to `theta` using SQL
- compute `cos(theta)` using Spark
- drop duplicated rows using Spark

here is how you would do it with `SparkChain`

```python title="pipeline.yaml"
import pandas as pd
from laktory import models

df0 = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 2, 3]}))

# Build Chain
sc = models.SparkChain(
    nodes=[
        {
            "sql_expr": "SELECT x AS theta FROM {df}",
        },
        {
            "with_column": {
                "name": "cos",
                "type": "double",
                "expr": "F.cos('theta')",
            },
        },
        {
            "spark_func_name": "dropDuplicates",
            "spark_func_args": ["theta"],
        },
    ]
)

# Execute Chain
df = sc.execute(df0, spark=spark)
```

In this example, `{df}` refers to the output of the previous node in the Spark Chain. You can also
directly reference other pipeline nodes in your SQL queries by using `{nodes.node_name}`.

For a more modular, scalable, and testable solution, we recommend using Spark over SQL. You can learn more in the blog 
posts
[Spark vs SQL](https://www.linkedin.com/pulse/sparkling-queries-in-depth-spark-vs-sql-data-pipelines-olivier-soucy-nfyve/)
and
[SparkChain](https://www.linkedin.com/pulse/laktory-sparkchain-serializable-spark-based-data-olivier-soucy-oihxe/)
.


### Polars Chain
??? "API Documentation"
    [`laktory.models.SparkChain`][laktory.models.SparkChain]<br>

The Polars chain is very similar to the Spark Chain as it defines a series of
core data transformations, except that it uses Polars instead of Spark as its
engine. The supported functions and syntax are also slightly different to
accommodate for Polars.
