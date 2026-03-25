from .basedatasink import BaseDataSink
from .datasinkwriter import DataSinkWriter
from .filedatasink import FileDataSink
from .hivemetastoredatasink import HiveMetastoreDataSink
from .mergecdcoptions import DataSinkMergeCDCOptions
from .pipelineviewdatasink import PipelineViewDataSink
from .tabledatasink import TableDataSink
from .tabledatasinkmetadata import TableDataSinkMetadata
from .unitycatalogdatasink import UnityCatalogDataSink

classes = [
    PipelineViewDataSink,
    FileDataSink,
    UnityCatalogDataSink,
    HiveMetastoreDataSink,
]


DataSinksUnion = (
    PipelineViewDataSink | FileDataSink | UnityCatalogDataSink | HiveMetastoreDataSink
)
