from abc import abstractmethod
from pyspark.sql import DataFrame

from laktory.models.base import BaseModel


class BaseSource(BaseModel):
    read_as_stream: bool | None = True

    @abstractmethod
    def read(self, spark) -> DataFrame:
        raise NotImplementedError()
