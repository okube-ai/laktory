import time
from typing import Union
from pyspark.sql.dataframe import DataFrame


def display(
    df: DataFrame,
    n: int = 10,
    truncate: Union[bool, int] = False,
    vertical: bool = False,
    refresh_interval: float = 3.0,
) -> None:
    """
    Prints the first n rows to the console. Compatible for both static and
    streaming dataframes. In the case of a streaming dataframe, console will
    be continuously updated until there is a keyboard input.

    Parameters
    ----------
    df:
        Input DataFrame
    n:
        Number of rows to display
    truncate:
        If set to `True`, truncate strings longer than 20 chars by default. If
        set to a number greater than one, truncates long strings to length
        truncate and align cells right.
    vertical:
        If set to `True`, print output rows vertically (one line per column
        value).
    refresh_interval:
        Pause duration, in seconds, between each update for streaming
        dataframes


    Returns
    -------
    :
        None

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
    df.laktory.display(n=5, refresh_interval=2)
    ```
    """

    if not df.isStreaming:
        df.show(n=n, truncate=truncate, vertical=vertical)

    else:

        # Start the streaming query
        query = (
            df.writeStream.outputMode("append")
            .format("memory")  # Store the results in-memory table
            .queryName("_laktory_tmp_view")
            .start()
        )

        try:
            while True:
                # Fetch and display the latest rows
                df.sparkSession.sql(f"SELECT * FROM _laktory_tmp_view LIMIT {n}").show(
                    truncate=truncate, vertical=vertical
                )
                time.sleep(refresh_interval)
        except KeyboardInterrupt:
            print("Stopped streaming display.")
        finally:
            query.stop()
