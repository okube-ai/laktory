from pydantic import model_validator
from laktory.spark import DataFrame
from typing import Literal
from typing import Any

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
    header: bool = True
    read_options: dict[str, str] = {}

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_storage(self, spark) -> DataFrame:
        if self.read_as_stream:
            logger.info(f"Reading {self.event_root} as stream")

            # Set reader
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", self.fmt)
                .option("cloudFiles.schemaLocation", self.event_root)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.allowOverwrites", True)
            )

        else:
            logger.info(f"Reading {self.event_root} as static")

            # Set reader
            reader = spark.read.format(self.fmt)

        reader = (
            reader.option("multiLine", self.multiline)  # only apply to JSON format
            .option("mergeSchema", True)
            .option("recursiveFileLookup", True)
            .option("header", self.header)  # only apply to CSV format
        )
        if self.read_options:
            reader = reader.options(**self.read_options)

        # Load
        df = reader.load(self.event_root)

        # Not supported by UC
        # .withColumn("file", F.input_file_name())

        return df

    def read(self, spark) -> DataFrame:
        if self.type == "STORAGE_EVENTS":
            return self._read_storage(spark)
        else:
            raise NotImplementedError()
