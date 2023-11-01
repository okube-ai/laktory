from laktory.spark import df_schema_flat
from pyspark.sql import SparkSession
import pandas as pd

# JAVA_HOME=/opt/homebrew/opt/java;SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec
pdf = pd.DataFrame({
    "x": [1, 2, 3],
    "y": [["a", "b"], ["b", "c"], ["c", "d"]],
    "z": [{"id": 3, "email": "@gmail.com"}, {"id": 2, "email": "@gmail.com"}, {"id": 1, "email": "@gmail.com"}],
    "u": [[{"a": 1, "b": 2}, {"a": 1, "b": 2}], [{"a": 1, "b": 2}, {"a": 1, "b": 2}], [{"a": 1, "b": 2}, {"a": 1, "b": 2}]],
    "u2": [[{"a": 1, "b": 2}, {"a": 1, "b": 2}], [{"a": 1, "b": 2}, {"a": 1, "b": 2}], [{"a": 1, "b": 2}, {"a": 1, "b": 2}]],
})

spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
#
# df = spark.createDataFrame(pdf)
#


def test_df_schema_flat():

    df = spark.createDataFrame(pdf)

    df.show()

    # schema = df_schema_flat(df)
    # print(schema)


if __name__ == "__main__":
    test_df_schema_flat()
