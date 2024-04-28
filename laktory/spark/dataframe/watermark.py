import json
from pyspark.sql.dataframe import DataFrame


def watermark(df: DataFrame) -> dict[str, str]:
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

    return {"column": column.strip(), "threshold": threshold.strip()}
