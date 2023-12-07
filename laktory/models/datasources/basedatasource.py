from abc import abstractmethod
from typing import Union
from laktory.spark import DataFrame

from laktory.models.basemodel import BaseModel


class BaseDataSource(BaseModel):
    """
    Base class for building data source

    Attributes
    ----------
    read_as_stream
        If `True` read source as stream
    """

    read_as_stream: Union[bool, None] = True

    @abstractmethod
    def read(self, spark) -> DataFrame:
        raise NotImplementedError()

    @property
    def is_cdc(self) -> bool:
        """If `True` source data is a change data capture (CDC)"""
        return getattr(self, "cdc", None) is not None
