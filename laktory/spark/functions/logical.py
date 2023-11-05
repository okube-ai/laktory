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
