# TODO: Figure out how to import BaseDataSource without creating a circular
# dependency
# from .basedatasource import BaseDataSource
# from .basedatasource import Watermark
from .eventdatasource import EventDataSource
from .tabledatasource import TableDataSource
from .tabledatasource import TableDataSourceCDC
