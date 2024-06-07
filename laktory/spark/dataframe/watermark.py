import re
import io
from contextlib import redirect_stdout
from pydantic import BaseModel

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


class Watermark(BaseModel):
    """
    Definition of a spark structured streaming watermark for joining data
    streams.

    Attributes
    ----------
    column:
        Event time column name
    threshold:
        How late, expressed in seconds, the data is expected to be with
        respect to event time.

    References
    ----------
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
    """

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
    import laktory  # noqa: F401
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

    watermark = df.laktory.watermark()
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
        # Some Databricks cluster types prevent from using private method
        # like ._jdf. Instead, we use .explain() method and capture the
        # output
        with io.StringIO() as buf, redirect_stdout(buf):
            df.explain(extended=True)
            plan = buf.getvalue().strip()

        lines = plan.split("\n")
        for line in lines:
            if "EventTimeWatermark".lower() in line.lower():
                c, t = line.lower().replace("'eventtimewatermark '", "").split(",")
                return Watermark(column=c.strip(), threshold=t.strip())
