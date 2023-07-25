import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from laktory.readers.eventsreader import EventsReader


class StreamEventsReader(EventsReader):

    def read(self, spark) -> DataFrame:
        return (
            spark
            .readStream.format("cloudFiles")
            .option("multiLine", False)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("cloudFiles.format", self.event.ingestion_pattern.fmt)
            .option("cloudFiles.schemaLocation", self.load_path)
            .option("cloudFiles.inferColumnTypes", True)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.allowOverwrites", True)
            .load(self.load_path)
        ).withColumn("file", F.input_file_name())
