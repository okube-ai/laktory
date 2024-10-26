from typing import Literal

from laktory.models.basemodel import BaseModel
from laktory._logger import get_logger

logger = get_logger(__name__)


class DataQualityCheck(BaseModel):
    """
    A Data Quality Check is the result of an expectation compared to a dataset.

    Attributes
    ----------
    fails_count:
        Number of rows not meeting the expectation.
    rows_count:
        Total number of rows in dataset.
    status:
        Result of comparison, considering the expectation criteria and
        tolerances.

    Examples
    --------
    ```py
    from laktory import models

    check = models.DataQualityCheck(
        rows_count=10,
        fails_count=2,
        status="FAIL",
    )
    print(check)
    #> variables={} fails_count=2 rows_count=10 status='FAIL'

    check = models.DataQualityCheck(
        rows_count=10,
        fails_count=2,
        status="PASS",
    )
    print(check)
    #> variables={} fails_count=2 rows_count=10 status='PASS'
    ```
    """

    fails_count: int = None
    rows_count: int = None
    status: Literal["PASS", "FAIL"]

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def failure_rate(self):
        """
        Ratio of the number of rows not meeting the expectation to the total number of rows in the dataframe. Returns
        `None` when the type is `"AGGREGATE"`.
        """
        # Empty DataFrame
        if self.rows_count == 0:
            return 0

        # Aggregate Expectation
        if self.fails_count is None:
            return None

        return self.fails_count / self.rows_count
