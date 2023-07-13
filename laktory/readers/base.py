from abc import abstractmethod

from pyspark.sql.dataframe import DataFrame
from laktory.models.ingestion_pattern import IngestionPattern


class BaseReader:
    def __init__(self, ingestion_pattern: IngestionPattern) -> None:
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass
