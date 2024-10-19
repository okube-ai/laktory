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
        """symbol == 'AAPL'""",
        'symbol == "AAPL"',
        "data.symbol",
        "data.created_at",
    ]:
        print("Test SQL: ", e)
        expr = models.DataFrameColumnExpression(value=e)
        _ = expr.eval()
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
        expr = models.DataFrameColumnExpression(value=e)
        assert expr.type == "DF"


def test_eval():

    # Spark
    e1 = models.DataFrameColumnExpression(value="symbol")
    e2 = models.DataFrameColumnExpression(value="F.col('symbol')")
    assert str(e1.eval()) == str(F.col("symbol"))
    assert str(e2.eval()) == str(F.col("symbol"))

    # Polars
    e1 = models.DataFrameColumnExpression(value="symbol")
    e2 = models.DataFrameColumnExpression(value="pl.col('symbol')")
    assert str(e1.eval(dataframe_type="POLARS")) == str(pl.col("symbol"))
    assert str(e2.eval(dataframe_type="POLARS")) == str(pl.col("symbol"))


if __name__ == "__main__":
    test_expression_types()
    test_eval()
