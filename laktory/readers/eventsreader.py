from pyspark.sql.dataframe import DataFrame

from laktory.readers.basereader import BaseReader
from laktory._settings import settings


class EventsReader(BaseReader):
    events_root_path: str = settings.landing_mount_path + "events"

    def read(self, spark, producer_name=None, event_name=None, load_path=None) -> DataFrame:

        if load_path is None:
            load_path = f"{self.events_root_path}/{producer_name}/{event_name}/"

        return (
            spark
            .read
            .option("multiLine", self.multiline)
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .format(self.fmt)
            .load(load_path)
        )

