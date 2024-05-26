from typing import Literal

from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel

logger = get_logger(__name__)


class PipelineNodeExpectation(BaseModel):
    """
    Definition of an expectation for a given node output DataFrame. It consists
    of an SQL expression of the expectation and of an action to be taken if the
    expectation is not met. Similar to DLT Table Expectation. Currently only
    supported with DLT pipeline orchestrator.

    Attributes
    ----------
    name:
        Name of the expectation
    expression:
        SQL expression definition the constraint
    action:
        Action to take when expectation is not met.
        `ALLOW`: Write invalid records to the output DataFrame, but log
        exception.
        `DROP`: Drop Invalid records to the output DataFrame and log exception.
        `FAIL`: Raise exception when invalid records are found.

    Examples
    --------
    ```py
    from laktory import models

    e = models.PipelineNodeExpectation(
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
