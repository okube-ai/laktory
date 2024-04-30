import json
from pydantic import BaseModel

from pyspark.sql.dataframe import DataFrame


class Watermark(BaseModel):
    column: str
    threshold: str


def watermark(df: DataFrame) -> Watermark:
    """
    Returns dataframe watermark if available

    Parameters
    ----------
    df:
        Input DataFrame

    Returns
    -------
    :
        Watermark column and threshold

    Examples
    --------
    ```py
    import pandas as pd

    df = spark.createDataFrame(
        pd.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "price": [200.0, 205.0],
                "tstamp": ["2023-09-01", "2023-09-01"],
            }
        )
    )
    df = df.withWatermark("tstamp", "1 hour")

    print(df.watermark())
    #> column='tstamp' threshold='1 hours'
    ```
    """
    plan = df._jdf.queryExecution().logical().toString().lower()

    lines = plan.split("\n")
    wm_found = False
    for line in lines:
        if "EventTimeWatermark".lower() in line.lower():
            wm_found = True
            break

    if not wm_found:
        return None

    line = line.lower().replace("'eventtimewatermark '", "")
    column, threshold = line.split(",")

    return Watermark(column=column.strip(), threshold=threshold.strip())
