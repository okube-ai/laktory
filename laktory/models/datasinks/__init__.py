from laktory.typing import tagged_union

from .basedatasink import BaseDataSink
from .customwriter import CustomWriter
from .filedatasink import FileDataSink
from .hivemetastoredatasink import HiveMetastoreDataSink
from .mergecdcoptions import DataSinkMergeCDCOptions
from .pipelineviewdatasink import PipelineViewDataSink
from .sharedoptions import DataSinkSharedOptions
from .tabledatasink import TableDataSink
from .tabledatasinkmetadata import TableDataSinkMetadata
from .unitycatalogdatasink import UnityCatalogDataSink

classes = [
    PipelineViewDataSink,
    FileDataSink,
    UnityCatalogDataSink,
    HiveMetastoreDataSink,
]


def _infer_sink(data: dict) -> type:
    # Frozen: new sink kinds must be selected through their `type`
    if "pipeline_view_name" in data:
        return PipelineViewDataSink
    if "path" in data:
        return FileDataSink
    return UnityCatalogDataSink


DataSinksUnion = tagged_union(classes, _infer_sink, "sink")
