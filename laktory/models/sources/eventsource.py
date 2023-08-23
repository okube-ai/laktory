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
    multiline: bool = False

    # tstamp_col: str = "created_at"

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
                .load(self.dirpath)
            )
        else:

            df = (
                spark
                .readStream.format("cloudFiles")
                .option("multiLine", self.multiline)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self.dirpath)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
                .load(self.dirpath)
            )

            # Not supported by UC
            # .withColumn("file", F.input_file_name())

        return df

    def read(self, spark) -> DataFrame:
        if self.type == "STORAGE_EVENTS":
            return self._read_storage(spark)
        else:
            raise NotImplementedError()
