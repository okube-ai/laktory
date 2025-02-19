import io
import sys

from pyspark.sql.dataframe import DataFrame


def is_aggregate(df: DataFrame) -> bool:
    """
    Check if DataFrame is an aggregation.

    Parameters
    ----------
    df:
        Input DataFrame

    Returns
    -------
    :
        Result

    Examples
    --------

    ```py
    import pyspark.sql.functions as F

    import laktory  # noqa: F401

    data = (
        {"symbol": "AAPL", "close": 1},
        {"symbol": "AAPL", "close": 2},
        {"symbol": "AAPL", "close": 3},
        {"symbol": "AMZN", "close": 4},
        {"symbol": "AMZN", "close": 5},
        {"symbol": "AMZN", "close": 6},
    )

    df = spark.createDataFrame(data)
    dfa = df.groupby("symbol").agg(F.mean("close"))

    dfa.laktory.is_aggregate()
    ```
    """
    buffer = io.StringIO()
    stdout0 = sys.stdout
    sys.stdout = buffer
    df.explain(mode="simple")
    sys.stdout = stdout0
    plan = buffer.getvalue()
    return "HashAggregate".lower() in plan.lower()
