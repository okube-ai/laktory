from pyspark.sql import SparkSession
import pandas as pd


pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})


spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.createDataFrame(pdf)

df.show()

