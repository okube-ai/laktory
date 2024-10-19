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
        if self.rows_count == 0:
            return 0
        return self.fails_count / self.rows_count

    @property
    def log_msg(self) -> str:
        msg = f"expr: {self.expectation.expr.value} | status: {self.status}"
        if self.expectation.type == "ROW":
            msg += f" | fails count: {self.fails_count} / {self.rows_count} ({100*self.failure_rate:5.2f} %)"
        return msg
