import polars as pl

from laktory.polars.dataframe.schema_flat import schema_flat
from laktory.polars.dataframe.has_column import has_column


@pl.api.register_expr_namespace("laktory")
class LaktoryExpression:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    # def hello(self) -> pl.Expr:
    #     return (pl.lit("Hello ") + self._expr).alias("hi there")
    #
    # def goodbye(self) -> pl.Expr:
    #     return (pl.lit("Sayōnara ") + self._expr).alias("bye")


# LaktoryExpression.has_column = has_column


@pl.api.register_dataframe_namespace("laktory")
class LaktoryDataFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    # def by_alternate_rows(self) -> list[pl.DataFrame]:
    #     df = self._df.with_row_index(name="n")
    #     return [
    #         df.filter((pl.col("n") % 2) == 0).drop("n"),
    #         df.filter((pl.col("n") % 2) != 0).drop("n"),
    #     ]


LaktoryDataFrame.has_column = has_column
LaktoryDataFrame.schema_flat = schema_flat
