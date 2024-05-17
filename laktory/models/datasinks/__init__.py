from typing import Union
from .basedatasink import BaseDataSink
from .filedatasink import FileDataSink
from .tabledatasink import TableDataSink

classes = [
    FileDataSink,
    TableDataSink,
]


DataSinksUnion = Union[
    FileDataSink,
    TableDataSink,
]
