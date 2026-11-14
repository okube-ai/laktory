from laktory.typing import tagged_union

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


def _infer_source(data: dict) -> type:
    # Frozen: new source kinds must be selected through their `type`
    if "node_name" in data:
        return PipelineNodeDataSource
    if "custom_reader" in data:
        return CustomDataSource
    if "df" in data or "data" in data:
        return DataFrameDataSource
    if "path" in data:
        return FileDataSource
    return UnityCatalogDataSource


DataSourcesUnion = tagged_union(classes, _infer_source, "source")
