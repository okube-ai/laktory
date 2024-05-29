import time
from pyspark.sql.dataframe import DataFrame


def display(df: DataFrame, rows: int = 5, refresh_interval: float = 3.0) -> bool:
    """
    Display both static and streaming dataframes

    Parameters
    ----------
    df:
        Input DataFrame
    rows:
        Number of rows to display
    refresh_interval:
        Pause duration, in seconds, between each update


    Returns
    -------
    :
        None

    Examples
    --------

    ```py
    import laktory  # noqa: F401
    import pyspark.sql.types as T
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
    df.display(rows=10)
    ```
    """

    if not df.isStreaming:
        df.limit(rows).show()

    else:

        # Start the streaming query
        query = (
            df.writeStream
            .outputMode("append")
            .format("memory")  # Store the results in-memory table
            .queryName("_laktory_tmp_view")
            .start()
        )

        try:
            while True:
                # Fetch and display the latest rows
                df.sparkSession.sql(f"SELECT * FROM _laktory_tmp_view LIMIT {rows}").show(truncate=False)
                time.sleep(refresh_interval)
        except KeyboardInterrupt:
            print("Stopped streaming display.")
        finally:
            query.stop()
