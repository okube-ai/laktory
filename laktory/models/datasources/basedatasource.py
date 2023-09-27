from abc import abstractmethod
from typing import Union
from laktory.spark import DataFrame

from laktory.models.base import BaseModel


class BaseDataSource(BaseModel):
    read_as_stream: Union[bool, None] = True

    @abstractmethod
    def read(self, spark) -> DataFrame:
        raise NotImplementedError()
