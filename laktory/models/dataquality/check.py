from typing import Any
from typing import Literal
from typing import Union
import warnings

from laktory.models.basemodel import BaseModel
from laktory.exceptions import DataQualityCheckFailedError
from laktory._logger import get_logger

logger = get_logger(__name__)


class DataQualityCheck(BaseModel):
    expectation: Any
    fails_count: int = None
    is_streaming: bool = None
    rows_count: int = None
    status: Literal["PASS", "FAIL", "UNDEFINED"]

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def failure_rate(self):
        if self.expectation.type != "ROW":
            return None
        if self.rows_count == 0:
            return 0
        if self.rows_count is None:
            return None
        return self.fails_count / self.rows_count

    @property
    def log_msg(self) -> str:
        msg = f"expr: {self.expectation.expr.value} | status: {self.status}"
        if self.expectation.type == "ROW":
            msg += f" | fails count: {self.fails_count} / {self.rows_count} ({100*self.failure_rate:5.2f} %)"
        return msg

    @property
    def action(self) -> str:
        return self.expectation.action

    @property
    def keep_filter(self) -> Union[str, None]:
        if self.expectation.type != "ROW":
            return None
        if self.action not in ["DROP", "QUARANTINE"]:
            return None
        if self.fails_count == 0:
            return None
        return self.expectation.pass_filter

    @property
    def quarantine_filter(self) -> Union[str, None]:
        if self.expectation.type != "ROW":
            return None
        if self.action not in ["QUARANTINE"]:
            return None
        if self.fails_count == 0:
            return None
        return self.expectation.fail_filter

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def raise_exception(self, node=None, warn=False) -> None:

        if self.is_streaming:
            return

        if self.status != "FAIL":
            return

        # Log failure
        msg = f"Expectation '{self.expectation.name}'"
        if node:
            msg += f" for node '{node.name}'"
        msg += f" FAILED | {self.log_msg}"

        # Raise Exception
        if self.action == "FAIL" and not warn:
            raise DataQualityCheckFailedError(self, node)
        else:
            # actions: WARN, DROP, QUARANTINE
            warnings.warn(msg)

