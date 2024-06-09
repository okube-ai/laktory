polars_installed = True
try:
    import polars
except ModuleNotFoundError:
    polars_installed = False


if polars_installed:
    from polars import DataFrame as PolarsDataFrame
    from polars import Expr as PolarsExpr

    import laktory.polars.dataframe
    import laktory.polars.expressions
    from laktory.polars.datatypes import DATATYPES_MAP

    def is_polars_dataframe(df):
        return isinstance(df, PolarsDataFrame)

else:
    # Mocks when polars is not installed

    class PolarsDataFrame:
        pass

    class PolarsExpr:
        pass

    def is_polars_dataframe(df):
        return False
