from .basedatasource import BaseDataSource
from .customdatasource import CustomDataSource
from .customreader import CustomReader
from .dataframedatasource import DataFrameDataSource
from .filedatasource import FileDataSource
from .hivemetastoredatasource import HiveMetastoreDataSource
from .pipelinenodedatasource import PipelineNodeDataSource
from .tabledatasource import TableDataSource
from .unitycatalogdatasource import UnityCatalogDataSource

classes = [
    CustomDataSource,
    FileDataSource,
    UnityCatalogDataSource,
    HiveMetastoreDataSource,
    DataFrameDataSource,
    PipelineNodeDataSource,
]

DataSourcesUnion = (
    CustomDataSource
    | FileDataSource
    | UnityCatalogDataSource
    | HiveMetastoreDataSource
    | DataFrameDataSource
    | PipelineNodeDataSource
)
