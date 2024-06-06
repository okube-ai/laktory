import polars as pl

from laktory.polars.expressions.math import roundp
from laktory.polars.expressions.sql import sql_expr


@pl.api.register_expr_namespace("laktory")
class LaktoryExpression:
    def __init__(self, expr: pl.Expr):
        self._expr = expr


LaktoryExpression.roundp = roundp
LaktoryExpression.sql_expr = sql_expr

# TODO: Enable?
# pl.expr.laktory = pl.Expr.laktory



