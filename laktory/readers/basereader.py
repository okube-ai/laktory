from typing import Literal
from abc import abstractmethod

from pyspark.sql.dataframe import DataFrame

from laktory.models.base import BaseModel
from laktory.models.ingestionpattern import IngestionPattern


FORMATS = (
    "JSON",
    "CSV",
    "PARQUET",
)


class BaseReader(BaseModel):
    fmt: Literal[FORMATS] = "JSON"
    multiline: bool = False

    @abstractmethod
    def read(self, spark) -> DataFrame:
        pass

