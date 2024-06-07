from pydantic import field_validator
from typing import Union

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_DATATYPES
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class Column(BaseModel):
    """
    Definition of a table column, including instructions on how to build the
    column using spark and an input dataframe.

    Attributes
    ----------
    catalog_name:
        Name of the catalog string the column table
    comment:
        Text description of the column
    name:
        Name of the column
    pii:
        If `True`, the column is flagged as Personally Identifiable Information
    schema_name:
        Name of the schema storing the column table
    table_name:
        Name of the table storing the column.
    type:
        Column data type
    unit:
        Column units
    """

    catalog_name: Union[str, None] = None
    comment: Union[str, None] = None
    name: str
    pii: Union[bool, None] = None
    raise_missing_arg_exception: Union[bool, None] = True
    schema_name: Union[str, None] = None
    table_name: Union[str, None] = None
    type: str = "string"
    unit: Union[str, None] = None

    @field_validator("type")
    def validate_type(cls, v: str) -> str:
        if v not in SUPPORTED_DATATYPES:
            raise ValueError(
                f"Type {v} is not supported. Select one of {SUPPORTED_DATATYPES}"
            )
        return v

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        """Table full name `{catalog_name}.{schema_name}.{table_name}`"""
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
        """Column full name `{catalog_name}.{schema_name}.{table_name}.{column_name}`"""
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id
