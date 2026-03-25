from typing import Literal

from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.models.datasources.basedatasource import BaseDataSource
from laktory.models.datasources.customreader import CustomReader
from laktory.typing import AnyFrame

logger = get_logger(__name__)


class CustomDataSource(BaseDataSource):
    """
    Data source backed by a fully user-supplied read function. Use this when
    no built-in source type fits — REST APIs, proprietary databases, custom
    formats, etc.

    Laktory calls the function and wraps the returned DataFrame in Narwhals.
    Post-read operations (filter, selects, renames, drops) are still applied
    by Laktory after the custom read.

    Examples
    --------
    ```py
    from laktory import models

    source = models.CustomDataSource(
        custom_reader={
            "func_name": "mypackage.etl.my_read",
            "func_kwargs": {"table": "catalog.schema.my_table"},
        },
    )
    df = source.read()
    ```

    ```py
    # mypackage/etl.py
    from laktory.models import LaktoryContext


    def my_read(table=None, laktory_context: LaktoryContext = None):
        import polars as pl

        return pl.scan_delta(table)
    ```

    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/sourcessinks/)
    """

    type: Literal["CUSTOM"] = Field("CUSTOM", frozen=True, description="Source type")
    custom_reader: CustomReader = Field(
        ...,
        description=(
            "Custom reader definition. Can be set as a plain string (func_name only) "
            "or a full CustomReader object with func_name, func_args, and func_kwargs."
        ),
    )

    @field_validator("custom_reader", mode="before")
    @classmethod
    def coerce_custom_reader(cls, v):
        if isinstance(v, str):
            return {"func_name": v}
        return v

    # ----------------------------------------------------------------------- #
    # Children                                                                #
    # ----------------------------------------------------------------------- #

    @property
    def children_names(self):
        return ["custom_reader"]

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return self.custom_reader.func_name

    # ----------------------------------------------------------------------- #
    # Read                                                                    #
    # ----------------------------------------------------------------------- #

    def _read(self, **kwargs) -> AnyFrame:
        return self.custom_reader.execute()
