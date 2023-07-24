from abc import abstractmethod

from pyspark.sql.dataframe import DataFrame

from laktory.models.base import BaseModel
from laktory.models.ingestionpattern import IngestionPattern


class BaseReader(BaseModel):

    @abstractmethod
    def read(self, spark) -> DataFrame:
        pass

