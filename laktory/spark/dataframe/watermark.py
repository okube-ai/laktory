import json
import re
from pydantic import BaseModel

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


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

    # Get plan
    if isinstance(df, ConnectDataFrame):
        plan = df._plan.print()

        def parse_watermark(input_string):
            match_c = re.search(r"event_time='([^']+)'", input_string)
            match_t = re.search(r"delay_threshold='([^']+)'", input_string)
            c = None
            t = None
            if match_c:
                c = match_c.group(1)
            if match_t:
                t = match_t.group(1)

            return c, t

        lines = plan.split("\n")
        for line in lines:
            if "<WithWatermark".lower() in line.lower():
                c, t = parse_watermark(line)
                return Watermark(column=c.strip(), threshold=t.strip())

    else:
        plan = df._jdf.queryExecution().logical().toString()

        lines = plan.split("\n")
        for line in lines:
            if "EventTimeWatermark".lower() in line.lower():
                c, t = line.lower().replace("'eventtimewatermark '", "").split(",")
                return Watermark(column=c.strip(), threshold=t.strip())
