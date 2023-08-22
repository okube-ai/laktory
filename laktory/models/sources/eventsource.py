from pyspark.sql import DataFrame
from typing import Literal

from laktory import settings
from laktory.models.eventheader import EventHeader
from laktory.models.sources.basesource import BaseSource


TYPES = (
    "STORAGE_DUMPS",
    "STORAGE_EVENTS",
    "STREAM_KINESIS",
    "STREAM_KAFKA",
)

FORMATS = (
    "JSON",
    "CSV",
    "PARQUET",
)


class EventSource(BaseSource, EventHeader):
    type: Literal[TYPES] = "STORAGE_EVENTS"
    fmt: Literal[FORMATS] = "JSON"
    # tstamp_col: str = "created_at"
    event_name: str | None = None
    producer_name: str | None = None
    events_root_path: str = settings.landing_mount_path + "events"
    load_path: str | None = None

    @property
    def _load_path(self) -> str:
        if self.load_path is not None:
            return self.load_path

        return f"{self.events_root_path}/{self.producer_name}/{self.event_name}"

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_storage(self, spark) -> DataFrame:

        if self.read_as_stream:
            df = (
                spark
                .read
                .option("multiLine", self.multiline)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .format(self.fmt)
                .load(self._load_path)
            )
        else:

            df = (
                spark
                .readStream.format("cloudFiles")
                .option("multiLine", self.multiline)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self._load_path)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
                .load(self._load_path)
            )

            # Not supported by UC
            # .withColumn("file", F.input_file_name())

        return df

    def read(self, spark) -> DataFrame:
        if self.type == "STORAGE_EVENTS":
            return self._read_storage(spark)
        else:
            raise NotImplementedError()
