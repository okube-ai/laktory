polars_installed = True
try:
    import polars
except ModuleNotFoundError:
    polars_installed = False


if polars_installed:
    from polars import DataFrame as PolarsDataFrame
    from polars import LazyFrame as PolarsLazyFrame
    from polars import Expr as PolarsExpr

    import laktory.polars.dataframe
    import laktory.polars.expressions
    from laktory.polars.datatypes import DATATYPES_MAP

    def is_polars_dataframe(df):
        """Check if dataframe is Polars DataFrame or Polars Lazy DataFrame"""
        if isinstance(df, PolarsDataFrame):
            return True

        if isinstance(df, PolarsLazyFrame):
            return True

        return False

else:
    # Mocks when polars is not installed

    class PolarsDataFrame:
        pass

    class PolarsLazyFrame:
        pass

    class PolarsExpr:
        pass

    def is_polars_dataframe(df):
        return False
