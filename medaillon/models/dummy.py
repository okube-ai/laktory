from typing import Literal

from pydantic import Field
from pydantic import ConfigDict

from medaillon.contants import SUPPORTED_TYPES
from medaillon.models.base import BaseModel


class Column(BaseModel):
    name: str
    type: Literal[tuple(SUPPORTED_TYPES.keys())] = "string"
    comment: str = None
    unit: str = None
    pii: bool = None
    udf_name: str = None
    input_cols: list[str] = []
    udf_kwargs: dict = {}
    test: int = Field(...)   # Set as required field, but put at end of list of fields

    model_config = ConfigDict(extra='forbid')


if __name__ == "__main__":
    speed = Column(
        name="airspeed",
        type="double",
        unit="kt",
        test=2,
    )
