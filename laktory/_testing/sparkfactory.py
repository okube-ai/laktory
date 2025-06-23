class SparkFactory:
    def __init__(self):
        self._spark = None

    @property
    def spark(self):
        if self._spark is None:
            from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder.appName("UnitTesting")
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .getOrCreate()
            )
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            self._spark = spark
        return self._spark
