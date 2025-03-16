from pyspark.sql import SparkSession

import laktory


def test_default_spark_session():
    assert not hasattr(laktory, "_spark")
    laktory.register_spark_session()
    spark = laktory.get_spark_session()
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.app.name") == "laktory"


def test_custom_spark_session():
    assert not hasattr(laktory, "_spark")
    laktory.register_spark_session(SparkSession.builder.appName("pytest").getOrCreate())
    spark = laktory.get_spark_session()
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.app.name") == "pytest"
