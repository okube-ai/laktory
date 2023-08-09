from typing import Literal
from pydantic import computed_field

from laktory.contants import SUPPORTED_TYPES
from laktory.models.base import BaseModel


class Column(BaseModel):
    name: str
    type: Literal[tuple(SUPPORTED_TYPES.keys())] = "string"
    comment: str = None
    catalog_name: str = None
    database_name: str = None
    table_name: str = None
    unit: str = None
    pii: bool = None
    func_name: str = None
    input_cols: list[str] = []
    func_kwargs: dict = {}

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @computed_field
    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.database_name:
            if _id == "":
                _id = self.database_name
            else:
                _id += f".{self.database_name}"

        if self.table_name:
            if _id == "":
                _id = self.table_name
            else:
                _id += f".{self.table_name}"

        return _id

    @computed_field
    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @computed_field
    @property
    def schema_name(self) -> str:
        return self.database_name

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    @classmethod
    def meta_table(cls):
        from laktory.models.table import Table
        columns = []
        for c in cls.model_fields.keys():
            columns += [
                Column(name=c, type="string")
            ]

        return Table(
            name="columns",
            database_name="laktory",
            columns=columns,
        )