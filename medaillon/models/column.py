from typing import Literal
from pydantic import computed_field

from medaillon.contants import SUPPORTED_TYPES
from medaillon.models.base import BaseModel


class Column(BaseModel):
    name: str
    type: Literal[tuple(SUPPORTED_TYPES.keys())] = "string"
    comment: str = None
    unit: str = None
    pii: bool = None
    func_name: str = None
    input_cols: list[str] = []
    func_kwargs: dict = {}
    parent_id: str = None

    @computed_field
    @property
    def table_name(self) -> str:
        if self.parent_id is None:
            return None
        return self.parent_id.split(".")[-1]

    @computed_field
    @property
    def schema_name(self) -> str:
        if self.parent_id is None or len(self.parent_id.split(".")) < 2:
            return None
        return self.parent_id.split(".")[-2]

    @computed_field
    @property
    def database_name(self) -> str:
        return self.schema_name

    @computed_field
    @property
    def catalog_name(self) -> str:
        if self.parent_id is None or len(self.parent_id.split(".")) < 3:
            return None
        return self.parent_id.split(".")[-3]


if __name__ == "__main__":
    speed = Column(
        name="airspeed",
        type="double",
        unit="kt",
        parent_id="lakehouse.flights.f012",
    )

    print(speed)
