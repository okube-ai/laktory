from laktory.spark import DataFrame
from typing import Union
from typing import Literal
from typing import Any

from laktory.models.base import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory._logger import get_logger

logger = get_logger(__name__)


class Watermark(BaseModel):
    column: str
    threshold: str


class TableDataSourceCDC(BaseModel):
    apply_as_deletes: Union[str, None] = None
    apply_as_truncates: Union[str, None] = None
    columns: Union[list[str], None] = []
    except_columns: Union[list[str], None] = []
    ignore_null_updates: Union[bool, None] = None
    primary_keys: list[str]
    scd_type: Literal[1, 2] = None
    sequence_by: str
    track_history_columns: Union[list[str], None] = None
    track_history_except_columns: Union[list[str], None] = None


class TableDataSource(BaseDataSource):
    _df: Any = None
    catalog_name: Union[str, None] = None
    cdc: Union[TableDataSourceCDC, None] = None
    selects: Union[list[str], dict[str, str], None] = None
    filter: Union[str, None] = None
    from_pipeline: Union[bool, None] = True
    name: Union[str, None]
    schema_name: Union[str, None] = None
    watermark: Union[Watermark, None] = None

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

    def _read(self, spark) -> DataFrame:
        from laktory.dlt import read
        from laktory.dlt import read_stream

        if self._df is not None:
            logger.info(f"Reading {self.full_name} from memory")
            df = self._df
        elif self.read_as_stream:
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

    def read(self, spark) -> DataFrame:
        import pyspark.sql.functions as F

        df = self._read(spark)

        # Apply filter
        if self.filter:
            df = df.filter(self.filter)

        # Columns
        cols = []
        if self.selects:
            if isinstance(self.selects, list):
                cols += [F.col(c) for c in self.selects]
            elif isinstance(self.selects, dict):
                cols += [F.col(k).alias(v) for k, v in self.selects.items()]
            df = df.select(cols)

        # Apply Watermark
        if self.watermark:
            df = df.withWatermark(
                self.watermark.column,
                self.watermark.threshold,
            )

        return df
