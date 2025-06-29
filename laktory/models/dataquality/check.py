from typing import Literal

from pydantic import Field

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class DataQualityCheck(BaseModel):
    """
    A Data Quality Check is the result of an expectation compared to a dataset.

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
    # > variables={} fails_count=2 rows_count=10 status='FAIL'

    check = models.DataQualityCheck(
        rows_count=10,
        fails_count=2,
        status="PASS",
    )
    print(check)
    # > variables={} fails_count=2 rows_count=10 status='PASS'
    ```
    References
    ----------
    * [Data Quality](https://www.laktory.ai/concepts/dataquality/)
    """

    fails_count: int = Field(
        None, description="Number of rows not meeting the expectation."
    )
    rows_count: int = Field(None, description="Total number of rows in dataset.")
    status: Literal["PASS", "FAIL"] = Field(
        ...,
        description="Result of comparison, considering the expectation criteria and tolerances.",
    )

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
