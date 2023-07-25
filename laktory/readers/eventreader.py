from pyspark.sql.dataframe import DataFrame

from laktory.readers.basereader import BaseReader
from laktory.models.eventdefinition import EventDefinition


class EventReader(BaseReader):
    event: EventDefinition

    def read(self, spark) -> DataFrame:
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
