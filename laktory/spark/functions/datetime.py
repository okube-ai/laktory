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
