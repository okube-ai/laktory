import re
from pyspark.sql.dataframe import DataFrame

from laktory.spark.dataframe.flat_schema import schema_flat


def has_column(df: DataFrame, col: str) -> bool:
    """
    Check if column `col` exists in `df`

    Parameters
    ----------
    df:
        Input DataFrame
    col
        Column name

    Returns
    -------
    :
        Result
    """
    _col = re.sub(r"\[(\d+)\]", r"[*]", col)
    _col = re.sub(r"`", "", _col)
    return _col in schema_flat(df)
