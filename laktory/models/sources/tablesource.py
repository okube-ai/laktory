from pyspark.sql import DataFrame

from laktory.dlt import read
from laktory.dlt import read_stream
from laktory.models.sources.basesource import BaseSource


class TableSource(BaseSource):
    name: str | None
    database_name: str | None = None
    catalog_name: str | None = None

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

        if self.read_as_stream:
            df = read_stream(self.full_name)
        else:
            df = read(self.full_name)

        return df
