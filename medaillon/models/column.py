from typing import Literal

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


if __name__ == "__main__":
    speed = Column(
        name="airspeed",
        type="double",
        unit="kt",
        test=2,
    )
