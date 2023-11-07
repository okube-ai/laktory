from laktory.spark import DataFrame
from typing import Union

from laktory.models.base import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory._logger import get_logger

logger = get_logger(__name__)


class TableDataSourceCDC(BaseModel):
    apply_as_deletes: Union[str, None] = None
    apply_as_truncates: Union[str, None] = None
    ignore_null_updates: Union[bool, None] = None
    primary_keys: list[str]
    sequence_by: str


class TableDataSource(BaseDataSource):
    catalog_name: Union[str, None] = None
    cdc: Union[TableDataSourceCDC, None] = None
    from_pipeline: Union[bool, None] = True
    name: Union[str, None]
    schema_name: Union[str, None] = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
        name = ""
        if self.catalog_name is not None:
            name = self.catalog_name

        if self.schema_name is not None:
            if name == "":
                name = self.schema_name
            else:
                name += f".{self.schema_name}"

        if name == "":
            name = self.name
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
            logger.info(f"Reading {self.full_name} as stream")
            if self.from_pipeline:
                df = read_stream(self.full_name)
            else:
                df = spark.readStream.format("delta").table(self.full_name)
        else:
            logger.info(f"Reading {self.full_name} as static")
            if self.from_pipeline:
                df = read(self.full_name)
            else:
                df = spark.read.table(self.full_name)

        return df
