from typing import Any
from typing import Union

from pydantic import Field
from pydantic import field_validator

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.models.dtypes import DType

logger = get_logger(__name__)


class DataFrameColumn(BaseModel):
    name: str = Field(None, description="Column name")
    dtype: Union[str, DType] = Field(..., description="Column data type")
    nullable: bool = Field(True, description="Column is nullable")
    is_primary: bool = Field(False, description="Column is a premiary key")

    @field_validator("dtype")
    def set_dtype(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = DType(name=v)
        return v
