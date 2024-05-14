from laktory.spark import SparkDataFrame
from typing import Union
from typing import Literal
from typing import Any
from pydantic import model_validator

from laktory.models.basemodel import BaseModel
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.spark import SparkDataFrame
from laktory._logger import get_logger

logger = get_logger(__name__)


class TableDataSource(BaseDataSource):
    """
    Data source using a data warehouse data table, generally used in the
    context of a data pipeline.

    Attributes
    ----------
    catalog_name:
        Name of the catalog of the source table
    cdc:
        Change data capture specifications
    from_dlt:
        If `True`, source will be read using `dlt.read` instead of `spark.read`
        when used in the context of a  Delta Live Table pipeline.
    schema_name:
        Name of the schema of the source table
    table_name:
        Name of the source table

    Examples
    ---------
    ```python
    from laktory import models

    source = models.TableDataSource(
        catalog_name="dev",
        schema_name="finance",
        table_name="brz_stock_prices",
        selects=["symbol", "open", "close"],
        filter="symbol='AAPL'",
        from_dlt=False,
        as_stream=True,
    )
    # df = source.read(spark)
    ```
    """

    catalog_name: Union[str, None] = None
    from_dlt: Union[bool, None] = False
    table_name: Union[str, None] = None
    schema_name: Union[str, None] = None
    warehouse: Union[Literal["DATABRICKS"], None] = "DATABRICKS"

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def full_name(self) -> str:
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

    # ----------------------------------------------------------------------- #
    # Readers                                                                 #
    # ----------------------------------------------------------------------- #

    def _read_spark(self, spark) -> SparkDataFrame:
        if self.warehouse == "DATABRICKS":
            return self._read_spark_databricks(spark)
        else:
            raise NotImplementedError(
                f"Warehouse '{self.warehouse}' is not yet supported."
            )

    def _read_spark_databricks(self, spark) -> SparkDataFrame:

        from laktory.dlt import read as dlt_read
        from laktory.dlt import read_stream as dlt_read_stream

        if self.as_stream:
            logger.info(f"Reading {self._id} as stream")
            if self.from_dlt:
                df = dlt_read_stream(self.full_name)
            else:
                df = spark.readStream.table(self.full_name)
        else:
            logger.info(f"Reading {self._id} as static")
            if self.from_dlt:
                df = dlt_read(self.full_name)
            else:
                df = spark.read.table(self.full_name)

        return df
