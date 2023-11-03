from typing import Union
import pyspark.sql.functions as F
# from pyspark.sql.functions import pandas_udf
from pyspark.sql.column import Column

__all__ = [
    "poly1",
    "poly2",
    "power",
    "roundp",
]

COLUMN_OR_NAME = Union[Column, str]
INT_OR_COLUMN = Union[int, COLUMN_OR_NAME]
FLOAT_OR_COLUMN = Union[float, COLUMN_OR_NAME]
STRING_OR_COLUMN = Union[str, Column]


def _col(col: str) -> Column:
    if isinstance(col, Column):
        return col

    return F.col(col)


def _lit(col: str) -> Column:
    if isinstance(col, Column):
        return col

    return F.lit(col)


# # --------------------------------------------------------------------------- #
# # compare                                                                     #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.BooleanType(), udfuncs)
# def compare(
#         x: pd.Series,
#         y: Union[float, pd.Series] = 0,
#         where: pd.Series = None,
#         operator: str = "==",
#         default: bool = None,
# ) -> pd.Series:
#     """
#     Compare a series `x` and a value or another series `y` using
#     `operator`. Comparison can be limited to `where` and assigned
#     `default` elsewhere.
#
#     output = `x` `operator` `y`
#
#     Parameters
#     ---------
#     x : pd.Series
#         Base series to compare
#     y : pd.Series or Object
#         Series or object to compare to
#     where: pd.Series
#         Where to apply the comparison
#     operator: str
#         Operator for comparison
#     default: bool
#         Default value to be applied outside of where
#
#     Returns
#     -------
#     output: pd.Series
#         Comparison result
#     """
#     output = pd.Series(default, x.index)
#
#     if where is None:
#         where = pd.Series(True, x.index)
#     elif isinstance(where, pd.Series):
#         where = where.astype(bool)
#
#     if not isinstance(y, pd.Series):
#         y = pd.Series(y, index=x.index)
#
#     if operator == "==":
#         output.loc[where] = x.loc[where] == y.loc[where]
#     elif operator == "!=":
#         output.loc[where] = x.loc[where] != y.loc[where]
#     elif operator == "<":
#         output.loc[where] = x.loc[where] < y.loc[where]
#     elif operator == "<=":
#         output.loc[where] = x.loc[where] <= y.loc[where]
#     elif operator == ">":
#         output.loc[where] = x.loc[where] > y.loc[where]
#     elif operator == ">=":
#         output.loc[where] = x.loc[where] >= y.loc[where]
#     else:
#         raise ValueError(f"Operator '{operator}' is not supported.")
#
#     return output
#

# --------------------------------------------------------------------------- #
# Polynomials                                                                 #
# --------------------------------------------------------------------------- #


def poly1(
        x: COLUMN_OR_NAME,
        a: FLOAT_OR_COLUMN = 1.0,
        b: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of first degree

    Parameters
    ----------
    x: pyspark.sql.functions.column.Column, column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        Slope
    b: float or pyspark.sql.functions.column.Column
        y-intercept

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """

    return _lit(a) * _col(x) + _lit(b)


def poly2(
        x: COLUMN_OR_NAME,
        a: FLOAT_OR_COLUMN = 1.0,
        b: FLOAT_OR_COLUMN = 0.0,
        c: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of second degree

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        x**2 coefficient
    b: float or pyspark.sql.functions.column.Column
        x**1 coefficient
    c: float or pyspark.sql.functions.column.Column
        x**0 coefficient

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """
    return _lit(a) * _col(x) ** 2 + _lit(b) * _col(x) + _lit(c)


# --------------------------------------------------------------------------- #
# power                                                                       #
# --------------------------------------------------------------------------- #

def power(
        x: COLUMN_OR_NAME,
        a: FLOAT_OR_COLUMN = 1.0,
        n: FLOAT_OR_COLUMN = 0.0,
) -> Column:
    """
    Polynomial function of first degree

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    a: float or pyspark.sql.functions.column.Column
        Coefficient
    n: float or pyspark.sql.functions.column.Column
        Exponent

    Returns
    -------
    output: pyspark.sql.functions.column.Column
        Output column
    """
    return _lit(a) * _col(x) ** _lit(n)


# --------------------------------------------------------------------------- #
# Rounding                                                                    #
# --------------------------------------------------------------------------- #

def roundp(
        x: COLUMN_OR_NAME,
        p: FLOAT_OR_COLUMN = 1.0,
) -> Column:
    """
    Evenly round to the given precision

    Parameters
    ------
    x: pyspark.sql.functions.column.Column or column name
        Input column
    p: float or pyspark.sql.functions.column.Column
        Precision

    Returns
    -------
    output: pd.Series
        Output series
    """
    # eps0 = 1.0e-16
    # precision = float(precision)
    # if precision < eps0:
    #     raise ValueError("Precision must be greater than 1.0e-16")
    return F.round(_col(x) / _lit(p)) * _lit(p)

# # --------------------------------------------------------------------------- #
# # string_split                                                                #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.StringType(), udfuncs)
# def string_split(
#     x: pd.Series,
#     pat: str = " ",
#     index: int = 0,
# ) -> pd.Series:
#     """
#     Get substring using separator `pat`.
#
#     Parameters
#     ------
#     x: pd.Series
#         Input text series to split
#     pat: str
#         String or regular expression to split on. If not specified, split on whitespace.
#     index: int
#         Split index to return
#
#     Returns
#     -------
#     output: pd.Series
#         Output series
#     """
#     return x.str.split(pat).apply(list.__getitem__, args=[index])
#
#
# # --------------------------------------------------------------------------- #
# # to_safe_timestamp                                                           #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.TimestampType(), udfuncs)
# def to_safe_timestamp(x: pd.Series, utc: bool=True) -> pd.Series:
#     """
#     Convert to timestamp when input data is a mix of:
#         - Timestamp string
#         - Unix timestamps in string (including exponential notation)
#         - Unix timestamps as floats
#
#     Parameters
#     ------
#     x: pd.Series
#         Input data
#
#     Returns
#     -------
#     output: pd.Series
#         Output series
#     """
#     x = x.copy()
#
#     # Find NaN
#     _x = x.str.lower().str
#     where_na = _x.contains("nan") | _x.contains("n/a") | _x.contains("null")
#
#     # Convert strings to datetime
#     where = x.str.contains("-") | x.str.contains(":") & ~where_na
#     _s = x.loc[where]
#     _s = pd.to_datetime(_s, utc=utc)
#
#     # Convert datetimes to unix timestamps
#     x.loc[where] = (_s - datetime(1970, 1, 1, tzinfo=pytz.utc)).dt.total_seconds().astype(pd.Float64Dtype())
#
#     # Set NaN
#     x.loc[where_na] = None
#
#     # Convert everything to datetimes
#     x = pd.to_datetime(x, unit="s")
#
#     return x
#
#
# # --------------------------------------------------------------------------- #
# # to_safe_timestamp                                                           #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.TimestampType(), udfuncs)
# def to_timestamp(x: pd.Series) -> pd.Series:
#     """
#     Convert to timestamp when input data is unix timestamps as floats
#
#     Parameters
#     ------
#     x: pd.Series
#         Input data
#
#     Returns
#     -------
#     output: pd.Series
#         Output series
#     """
#     # Convert everything to datetimes
#     x = pd.to_datetime(x, unit="s")
#     return x
#
#
# # --------------------------------------------------------------------------- #
# # unix_timestamp                                                              #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.DoubleType(), udfuncs)
# def unix_timestamp(
#     x: pd.Series,
# ) -> pd.Series:
#     """
#     Return current unix timestamp in seconds
#
#     Parameters
#     ----------
#     x: pd.Series, default = None
#         Series used to set index
#
#     Returns
#     -------
#     output: float, pd.Series
#         Current timestamp
#     """
#     return pd.Series(_unix_timestamp(), index=x.index)
#
#
# # --------------------------------------------------------------------------- #
# # uuid                                                                        #
# # --------------------------------------------------------------------------- #
#
# @pandas_udf(sqlt.StringType(), udfuncs)
# def uuid(
#     x: pd.Series,
# ) -> pd.Series:
#     """
#     Create a unique id for each row.
#
#     Parameters
#     ------
#     x: pd.Series
#         Series used to set index
#
#     Returns
#     -------
#     output: str, pd.Series
#         Unique identified
#     """
#     return pd.Series([str(_uuid.uuid4()) for _ in x], index=x.index)
