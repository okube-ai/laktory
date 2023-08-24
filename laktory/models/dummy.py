from typing import Literal
from typing import Union

from pydantic import Field
from pydantic import ConfigDict

from laktory.contants import SUPPORTED_TYPES
from laktory.models.base import BaseModel


class Column(BaseModel):
    name: str
    type: Literal[tuple(SUPPORTED_TYPES.keys())] = "string"
    comment: Union[str, None] = None
    unit: Union[str, None] = None
    pii: Union[bool, None] = None
    udf_name: Union[str, None] = None
    input_cols: list[str] = []
    udf_kwargs: dict = {}
    test: int = Field(...)  # Set as required field, but put at end of list of fields

    model_config = ConfigDict(extra="forbid")


if __name__ == "__main__":
    speed = Column(
        name="airspeed",
        type="double",
        unit="kt",
        test=2,
    )
