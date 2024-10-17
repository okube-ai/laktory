from pyspark.sql import functions as F
import polars as pl

from laktory import models
from laktory._testing import spark


def test_expression_types():

    for e in [
        "symbol",
        "MAX(close)",
        "close > 30",
        "symbol == 'AAPL'",
        "data.symbol",
        "data.created_at",
        """
        SELECT
          data.created_at AS created_at,
          data.symbol AS symbol,
          data.open AS open,
          data.close AS close,
          data.high AS high,
          data.low AS low,
          data.volume AS volume
        FROM
          {df}
        """,
    ]:
        print("Test SQL: ", e)
        expr = models.DataFrameExpression(expr=e)
        assert expr.type == "SQL"

    for e in [
        "pl.expr.laktory.sql_expr('data._created_at')",
        "F.max('close')",
        "F.expr('data._created_at')",
        "F.last('close')",
        "F.col('data.symbol')",
        "pl.col('created_at').dt.truncate('1d')",
        "pl.col('open').first()",
    ]:
        print("Test DF: ", e)
        expr = models.DataFrameExpression(expr=e)
        assert expr.type == "DF"


def test_eval():

    # Spark
    e1 = models.DataFrameExpression(expr="symbol")
    e2 = models.DataFrameExpression(expr="F.col('symbol')")
    assert str(e1.eval()) == str(F.col("symbol"))
    assert str(e2.eval()) == str(F.col("symbol"))

    # Polars
    e1 = models.DataFrameExpression(expr="symbol")
    e2 = models.DataFrameExpression(expr="pl.col('symbol')")
    assert str(e1.eval(dataframe_type="POLARS")) == str(pl.col("symbol"))
    assert str(e2.eval(dataframe_type="POLARS")) == str(pl.col("symbol"))


if __name__ == "__main__":
    test_expression_types()
    test_eval()
