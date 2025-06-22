from .basedatasink import BaseDataSink
from .filedatasink import FileDataSink
from .hivemetastoredatasink import HiveMetastoreDataSink
from .mergecdcoptions import DataSinkMergeCDCOptions
from .pipelineviewdatasink import PipelineViewDataSink
from .tabledatasink import TableDataSink
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
