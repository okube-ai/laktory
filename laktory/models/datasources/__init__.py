from .basedatasource import BaseDataSource
from .dataframedatasource import DataFrameDataSource
from .filedatasource import FileDataSource
from .hivemetastoredatasource import HiveMetastoreDataSource
from .pipelinenodedatasource import PipelineNodeDataSource
from .tabledatasource import TableDataSource
from .unitycatalogdatasource import UnityCatalogDataSource

classes = [
    FileDataSource,
    UnityCatalogDataSource,
    HiveMetastoreDataSource,
    DataFrameDataSource,
    PipelineNodeDataSource,
]

DataSourcesUnion = (
    FileDataSource
    | UnityCatalogDataSource
    | HiveMetastoreDataSource
    | DataFrameDataSource
    | PipelineNodeDataSource
)
