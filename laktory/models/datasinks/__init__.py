from .basedatasink import BaseDataSink
from .dltviewdatasink import DLTViewDataSink
from .filedatasink import FileDataSink
from .hivemetastoredatasink import HiveMetastoreDataSink
from .mergecdcoptions import DataSinkMergeCDCOptions
from .tabledatasink import TableDataSink
from .unitycatalogdatasink import UnityCatalogDataSink

classes = [
    DLTViewDataSink,
    FileDataSink,
    UnityCatalogDataSink,
    HiveMetastoreDataSink,
]


DataSinksUnion = (
    DLTViewDataSink | FileDataSink | UnityCatalogDataSink | HiveMetastoreDataSink
)
