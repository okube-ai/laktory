??? "API Documentation"
    [`laktory.spark.functions`](../api/spark/functions/poly1.md)<br>
    [`laktory.spark.dataframe`](../api/spark/dataframe/has_column.md)<br>

Apache Spark, an open-source, distributed computing system. Apache Spark is designed for big data processing and analytics and provides a fast and general-purpose cluster-computing framework.
It supports various programming languages, including Scala, Java, Python, and R.

To facilitate the transformation of your data Laktory extends spark native functions.

## Functions
The first extension is the provision of a library of functions that can be used to build columns from other columns or constants.

```py
import pandas as pd
import laktory.spark.functions as LF

df = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))
df = df.withColumn("y", LF.poly1("x", -1, 1.0))
```
These functions are by default available when declaring a column in a pipeline `Table` model.

## DataFrame
In this case the methods are designed to be applied directly on a spark dataframe.
```py
import pandas as pd
from laktory.spark.dataframe import has_column

df = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))
has_column(df, "x")
```

Laktory is monkey patching the DataFrame class from spark by assigning all the custom methods at runtime. In other words,
you can also do the following to get the same results
```py
import pandas as pd

df = spark.createDataFrame(pd.DataFrame({"x": [1, 2, 3]}))
df.has_column("x")
```
