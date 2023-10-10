from typing import Union
from pydantic import computed_field
from pydantic import field_validator

from laktory.contants import SUPPORTED_TYPES
from laktory.models.base import BaseModel
from laktory.sql import py_to_sql


class Column(BaseModel):
    name: str
    type: str = "string"
    comment: Union[str, None] = None
    catalog_name: Union[str, None] = None
    schema_name: Union[str, None] = None
    table_name: Union[str, None] = None
    unit: Union[str, None] = None
    pii: Union[bool, None] = None
    func_name: Union[str, None] = None
    input_cols: list[str] = []
    func_kwargs: dict[str, Union[str, None]] = {}
    jsonize: bool = False

    @field_validator("type")
    def default_load_path(cls, v: str) -> str:
        if "<" in v:
            return v
        else:
            if v not in SUPPORTED_TYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_TYPES}"
                )
        return v

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        if self.table_name:
            if _id == "":
                _id = self.table_name
            else:
                _id += f".{self.table_name}"

        return _id

    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @property
    def database_name(self) -> str:
        return self.schema_name

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    @classmethod
    def meta_table(cls):
        from laktory.models.table import Table

        # Build columns
        columns = []
        for k, t in cls.model_serialized_types().items():
            jsonize = False
            if k in ["func_kwargs"]:
                t = "string"
                jsonize = True

            columns += [
                Column(name=k, type=py_to_sql(t, mode="schema"), jsonize=jsonize)
            ]

        # Set table
        return Table(
            name="columns",
            schema_name="laktory",
            columns=columns,
        )
