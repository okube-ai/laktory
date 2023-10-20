from laktory.spark import DataFrame
from typing import Literal

from laktory.models.dataeventheader import DataEventHeader
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory._logger import get_logger

logger = get_logger(__name__)

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


class EventDataSource(BaseDataSource, DataEventHeader):
    type: Literal[TYPES] = "STORAGE_EVENTS"
    fmt: Literal[FORMATS] = "JSON"
    multiline: bool = False

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_storage(self, spark) -> DataFrame:
        if self.read_as_stream:
            logger.info(f"Reading {self.event_root} as stream")
            df = (
                spark.readStream.format("cloudFiles")
                .option("multiLine", self.multiline)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self.event_root)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
                .load(self.event_root)
            )
        else:
            logger.info(f"Reading {self.event_root} as static")
            df = (
                spark.read.option("multiLine", self.multiline)
                .option("mergeSchema", True)
                .option("recursiveFileLookup", True)
                .format(self.fmt)
                .load(self.event_root)
            )
            # Not supported by UC
            # .withColumn("file", F.input_file_name())

        return df

    def read(self, spark) -> DataFrame:
        if self.type == "STORAGE_EVENTS":
            return self._read_storage(spark)
        else:
            raise NotImplementedError()
