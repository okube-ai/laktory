from .basedatasink import BaseDataSink
from .filedatasink import FileDataSink
from .hivemetastoredatasink import HiveMetastoreDataSink
from .mergecdcoptions import DataSinkMergeCDCOptions
from .tabledatasink import TableDataSink
from .unitycatalogdatasink import UnityCatalogDataSink

classes = [
    FileDataSink,
    UnityCatalogDataSink,
    HiveMetastoreDataSink,
]


DataSinksUnion = FileDataSink | UnityCatalogDataSink | HiveMetastoreDataSink
