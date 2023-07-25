from pyspark.sql.dataframe import DataFrame

from laktory.readers.base import BaseReader
from laktory.models.eventdefinition import EventDefinition


class EventReader(BaseReader):
    event: EventDefinition

    def read(self, spark) -> DataFrame:

        if self.event.ingestion_pattern.read_as_stream:
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
            )

        else:
            return (
                spark
                .read
                .option("multiLine", False)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .format(self.event.ingestion_pattern.fmt)
                .load(self.load_path)
            )

    @property
    def load_path(self):
        return self.event.landing_dirpath
