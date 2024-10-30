from databricks.connect import DatabricksSession
from laktory import models


# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

spark = DatabricksSession.builder.clusterId("0709-135558-ql8jcu2x").getOrCreate()


# --------------------------------------------------------------------------- #
# Get Pipeline                                                                #
# --------------------------------------------------------------------------- #

df = spark.read.table("dev.meteo.config_envcan_station")

df.printSchema()