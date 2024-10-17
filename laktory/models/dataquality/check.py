from typing import Any
from typing import Literal

from laktory.models.basemodel import BaseModel
from laktory._logger import get_logger

logger = get_logger(__name__)


class DataQualityCheck(BaseModel):
    expectation: Any
    fails_count: int = None
    rows_count: int = None
    status: Literal["PASS", "FAIL"]

    @property
    def failure_rate(self):
        if self.expectation.type != "ROW":
            return None
        return self.fails_count / self.rows_count
