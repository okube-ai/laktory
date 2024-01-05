from typing import Literal

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class TableExpectation(BaseModel):
    """
    Specifications for aggregation by group or time window.

    Attributes
    ----------
    name:
        Name of the expectation
    expression:
        SQL expression definition the constraint
    action:
        Action to take when expectation is not met.
        `ALLOW`: Invalid records are written to the target; failure is reported
                 as a metric for the dataset.
        `DROP`: Invalid records are dropped before data is written to the
                target; failure is reported as a metrics for the dataset.
        `FAIL`: Invalid records prevent the update from succeeding. Manual
                intervention is required before re-processing.

    Examples
    --------
    ```py
    from laktory import models

    e = models.TableExpectation(
        name="valid timestamp",
        expression="col(“timestamp”) > '2012-01-01'",
        action="DROP",
    )
    print(e)
    '''
    variables={} name='valid timestamp' expression="col(“timestamp”) > '2012-01-01'" action='DROP'
    '''
    ```

    References
    ----------

    * [DLT Table Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
    """

    name: str
    expression: str
    action: Literal["ALLOW", "DROP", "FAIL"] = "ALLOW"
