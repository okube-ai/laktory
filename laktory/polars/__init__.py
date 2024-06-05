try:
    import polars

    from polars import DataFrame as PolarsDataFrame
    from polars import Expr as PolarsExpr

    import laktory.polars.dataframe
    import laktory.polars.expressions
    from .expressions import sql_expr

    def is_polars_dataframe(df):
        return isinstance(df, PolarsDataFrame)

except ModuleNotFoundError:
    # Mocks when polars is not installed

    class PolarsDataFrame:
        pass

    class PolarsExpr:
        pass

    def is_polars_dataframe(df):
        return False
