import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from laktory.readers.eventsreader import EventsReader


class EventsStreamReader(EventsReader):

    def read(self, spark) -> DataFrame:
        return (
            spark
            .readStream.format("cloudFiles")
            .option("multiLine", self.multiline)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("cloudFiles.format", self.fmt)
            .option("cloudFiles.schemaLocation", self.load_path)
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.allowOverwrites", True)
            .load(self.load_path)
        )

        # Not supported by UC
        # .withColumn("file", F.input_file_name())
