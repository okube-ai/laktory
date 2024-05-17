from typing import Union
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


def create_union(*args):
    return Union[args]


DataSourcesUnion = create_union(*classes)
