from typing import Union

from pyspark.sql.dataframe import DataFrame


def show_string(
    df: DataFrame,
    n: int = 20,
    truncate: Union[bool, int] = True,
    vertical: bool = False,
) -> str:
    """
    Returns the string generated by `df.show()`

    Parameters
    ----------
    n:
        Number of rows to show.
    truncate:
        If set to `True`, truncate strings longer than 20 chars by default. If
        set to a number greater than one, truncates long strings to length
        truncate and align cells right.
    vertical:
        If set to True, print output rows vertically (one line per column
        value).

    Returns
    -------
    :
        Show string

    Examples
    --------

    ```py
    import laktory  # noqa: F401

    df = spark.createDataFrame([[7, "a"], [8, "b"], [9, "c"]], ["x", "y"])
    print(df.laktory.show_string())
    '''
    +---+---+
    |  x|  y|
    +---+---+
    |  7|  a|
    |  8|  b|
    |  9|  c|
    +---+---+
    '''
    ```
    """
    if truncate is True:
        truncate = 20
    return df._jdf.showString(n, int(truncate), vertical)
