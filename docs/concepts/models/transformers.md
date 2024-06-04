<img src="/../../images/transformer_logo.png" alt="node transformer" width="100"/>

### Spark Chain
??? "API Documentation"
    [`laktory.models.SparkChain`][laktory.models.SparkChain]<br>

Spark chain is the core dataframe transformers model. It is essentially the
serialization of chained spark operations with support for [data sources](sourcessinks.md)
in addition to dataframes.

<img src="/../../images/spark_chain_diagram.png" alt="node transformer" width="600"/>

A Spark chain is defined as a series of nodes, each one representing a
transformation applied to a dataframe. A node declare the spark function
responsible for the transformation and the arguments to pass to that function.
Each function is expected to output a dataframe and receive as an input the
output of the previous node. As a convenience, a node can also declare a new
column. In this case, the function is expected to output a column.

For example, consider a simple dataframe with column `x` for which you want to:

- rename `x` to `theta`
- compute `cos(theta)`
- drop duplicated rows

here is how you would do it with `SparkChain`

```python title="pipeline.yaml"
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

For more information about why we promote usage of Spark over SQL and to go
over many of the benefits offered by SparkChain, have a look at these
[Spark vs SQL](https://www.linkedin.com/pulse/sparkling-queries-in-depth-spark-vs-sql-data-pipelines-olivier-soucy-nfyve/)
and
[SparkChain](https://www.linkedin.com/pulse/laktory-sparkchain-serializable-spark-based-data-olivier-soucy-oihxe/)
blog posts.