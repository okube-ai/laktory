from typing import Union
from .basedatasource import BaseDataSource
from .filedatasource import FileDataSource
from .memorydatasource import MemoryDataSource
from .pipelinenodedatasource import PipelineNodeDataSource
from .tabledatasource import TableDataSource

classes = [
    FileDataSource,
    MemoryDataSource,
    PipelineNodeDataSource,
    TableDataSource,
]

DataSourcesUnion = Union[
    FileDataSource,
    MemoryDataSource,
    PipelineNodeDataSource,
    TableDataSource,
]
