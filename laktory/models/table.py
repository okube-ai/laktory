from typing import Literal

from pydantic import computed_field

from laktory.models.base import BaseModel
from laktory.models.column import Column
from laktory.models.eventdefinition import EventDefinition


class Table(BaseModel):
    name: str
    columns: list[Column] = []
    primary_key: str = None
    comment: str = None
    parent_id: str = None

    # Lakehouse
    input_event: str = None
    input_table: str = None
    zone: Literal["BRONZE", "SILVER", "SILVER_STAR", "GOLD"] = None
    # joins
    # expectations

    @computed_field
    @property
    def database_name(self) -> str:
        if self.parent_id is None or len(self.parent_id.split(".")) < 1:
            return None
        return self.parent_id.split(".")[-1]

    @computed_field
    @property
    def schema_name(self) -> str:
        return self.database_name

    @computed_field
    @property
    def catalog_name(self) -> str:
        if self.parent_id is None or len(self.parent_id.split(".")) < 2:
            return None
        return self.parent_id.split(".")[-2]


if __name__ == "__main__":
    table = Table(
        name="f1549",
        columns=[
            {
                "name": "airspeed",
                "type": "double",
            },
            {
                "name": "altitude",
                "type": "double",
            },
        ],
    )

    print(table)
