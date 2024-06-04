try:
    import polars

    from polars import DataFrame as PolarsDataFrame

    from .expressions import sql_expr

    def is_polars_dataframe(df):
        return isinstance(df, PolarsDataFrame)

except ModuleNotFoundError:
    # Mocks when polars is not installed

    class PolarsDataFrame:
        pass

    def is_polars_dataframe(df):
        return False
