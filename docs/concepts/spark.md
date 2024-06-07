??? "API Documentation"
    [`laktory.spark.functions`](../api/spark/functions/poly1.md)<br>
    [`laktory.spark.dataframe`](../api/spark/dataframe/has_column.md)<br>

Apache Spark, an open-source, distributed computing system. Apache Spark is designed for big data processing and analytics and provides a fast and general-purpose cluster-computing framework.
It supports various programming languages, including Scala, Java, Python, and R.

To facilitate the transformation of your data Laktory extends spark native functions by in a `laktory` namespace.

## Functions
The first extension is the provision of a library of functions that can be used to build columns from other columns or constants.

```py
import laktory
import pandas as pd
import pyspark.sql.functions as F

df = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))
df = df.withColumn("y", F.laktory.poly1("x", -1, 1.0))
```
Here function `poly1` is a Laktory-specific function and is available because of the `import laktory` statement. All 
other custom functions are also available from the `pyspark.sql.functions.laktory` namespace.

## Dataframe methods
In this case the methods are designed to be applied directly on a spark dataframe.
```py
import laktory
import pandas as pd

df = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))
df.laktory.has_column("x")
```

Laktory is monkey patching the DataFrame class from spark by assigning all the custom methods under the `laktory`
namespace at runtime. 

Some methods of interest are:

- [`laktory.spark.dataframe.groupby_and_agg`](../api/spark/dataframe/groupby_and_agg.md): Apply a groupby and create aggregation columns.
- [`laktory.spark.dataframe.smart_join`](../api/spark/dataframe/smart_join.md): Join tables, clean up duplicated columns and support watermarking for streaming joins. This is the recommended spark function for all joins defined in pipelines.
- [`laktory.spark.dataframe.window_filter`](../api/spark/dataframe/window_filter.md): Apply spark window-based filtering
