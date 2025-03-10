from typing import Union

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource

logger = get_logger(__name__)


class TableDataSource(BaseDataSource):
    catalog_name: Union[str, None] = None
    table_name: Union[str, None] = None
    schema_name: Union[str, None] = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        """Table full name {catalog_name}.{schema_name}.{table_name}"""
        if self.table_name is None:
            return None

        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.schema_name is not None:
            if name == "":
                name = self.schema_name
            else:
                name += f".{self.schema_name}"

        if name == "":
            name = self.table_name
        else:
            name += f".{self.table_name}"

        return name

    @property
    def _id(self) -> str:
        return self.full_name
