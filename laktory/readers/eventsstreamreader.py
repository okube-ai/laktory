import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from laktory.readers.eventsreader import EventsReader


class EventsStreamReader(EventsReader):

    def read(self, spark, producer_name=None, event_name=None, load_path=None) -> DataFrame:

        if load_path is None:
            load_path = f"{self.events_root_path}/{producer_name}/{event_name}/"

        return (
            spark
            .readStream.format("cloudFiles")
            .option("multiLine", self.multiline)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("cloudFiles.format", self.fmt)
            .option("cloudFiles.schemaLocation", load_path)
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.allowOverwrites", True)
            .load(load_path)
        ).withColumn("file", F.input_file_name())
