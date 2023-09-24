from pyspark.sql import DataFrame
from typing import Union

from laktory.models.datasources.basedatasource import BaseDataSource


class TableDataSource(BaseDataSource):
    name: Union[str, None]
    database_name: Union[str, None] = None
    catalog_name: Union[str, None] = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.database_name is not None:
            if name == "":
                name = self.database_name
            else:
                name += f".{self.database_name}"

        if name == "":
            name = self.table_name
        else:
            name += f".{self.name}"

        return name

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def read(self, spark) -> DataFrame:
        from laktory.dlt import read
        from laktory.dlt import read_stream

        if self.read_as_stream:
            df = read_stream(self.full_name)
        else:
            df = read(self.full_name)

        return df
