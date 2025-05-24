from __future__ import annotations

from typing import Any

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class ReaderWriterMethod(BaseModel):
    """
    DataFrame Backend Method

    Examples
    --------
    ```py
    from laktory import models

    m = models.DataFrameBackendMethod(name="format", args=["CSV"])
    ```
    """

    name: str = Field(..., description="Method name")
    args: list[Any] = Field([], description="Method arguments")
    kwargs: dict[str, Any] = Field({}, description="Method keyword arguments")

    @property
    def as_string(self) -> str:
        s = f"{self.name}("
        s += ",".join([str(v) for v in self.args])
        s += ",".join([f"{k}={v}" for k, v in self.kwargs.items()])
        s += ")"
        return s
