from typing import Union

from .basedatasource import BaseDataSource
from .dataframedatasource import DataFrameDataSource
from .filedatasource import FileDataSource
from .pipelinenodedatasource import PipelineNodeDataSource
from .tabledatasource import TableDataSource
from .unitycatalogdatasource import UnityCatalogDataSource

classes = [
    DataFrameDataSource,
    FileDataSource,
    PipelineNodeDataSource,
    UnityCatalogDataSource,
]

DataSourcesUnion = Union[
    DataFrameDataSource,
    FileDataSource,
    PipelineNodeDataSource,
    UnityCatalogDataSource,
]
