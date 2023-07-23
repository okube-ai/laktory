from abc import abstractmethod

from pyspark.sql.dataframe import DataFrame
from laktory.models.ingestion_pattern import IngestionPattern


class BaseReader:
    def __init__(self, ingestion_pattern: IngestionPattern) -> None:
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass
        # return (
        #     spark
        #     .read
        #     .format("json")
        #     .option("multiLine", False)
        #     # .option("mergeSchema", True)
        #     # .option("cloudFiles.format", ip.fmt)
        #     # .option("cloudFiles.schemaLocation", data_path)
        #     # .option("cloudFiles.inferColumnTypes", True)
        #     # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        #     # .option("cloudFiles.allowOverwrites", True)
        #     # .option("recursiveFileLookup", True)
        #     .load(filepath)
        # )
