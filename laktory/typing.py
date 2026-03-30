from typing import TYPE_CHECKING

import narwhals as nw
from pydantic_core import CoreSchema
from pydantic_core import core_schema

if TYPE_CHECKING:
    pass

AnyFrame = nw.LazyFrame | nw.DataFrame


class VariableType(str):
    """
    Laktory variable ${vars.my_variable_name} or expression ${{ vars.id == 2}} string

    References
    ----------
    * [Data Sources and Sinks](https://www.laktory.ai/concepts/variables/)
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type, handler: callable
    ) -> CoreSchema:
        return core_schema.str_schema()
