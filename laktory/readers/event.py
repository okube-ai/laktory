from pyspark.sql.dataframe import DataFrame

from laktory.readers.base import BaseReader
from laktory.models.eventdefinition import EventDefinition


class EventReader(BaseReader):
    event: EventDefinition

    def read(self, spark) -> DataFrame:
        return (
            spark
            .read
            .format(self.event.ingestion_pattern.fmt)
            .option("multiLine", False)
            # .option("mergeSchema", True)
            # .option("cloudFiles.format", ip.fmt)
            # .option("cloudFiles.schemaLocation", data_path)
            # .option("cloudFiles.inferColumnTypes", True)
            # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            # .option("cloudFiles.allowOverwrites", True)
            # .option("recursiveFileLookup", True)
            .load(self.load_path)
        )

    @property
    def load_path(self):
        return self.event.landing_dirpath
