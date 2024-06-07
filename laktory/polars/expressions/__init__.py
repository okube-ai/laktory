from functools import wraps
import polars as pl

from laktory.polars.expressions.math import roundp
from laktory.polars.expressions.sql import sql_expr


def _parse_expr(expr):
    """
    Polars expressions are designed to be chained to each other. When an
    extension expression is called, the calling expression is stored as
    `_expr` attribute. To support both chained operations as
    `pl.col("x").laktory.roundp(p=2.0)`
    and stand alone operations as
    `pl.Expr.laktory.roundp(pl.col("x"), p=2.0)`

    we check the value first argument received in an expression and return
    value._expr when appropriate.
    """
    if isinstance(expr, LaktoryExpression):
        return expr._expr
    return expr


@pl.api.register_expr_namespace("laktory")
class LaktoryExpression:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    @wraps(roundp)
    def roundp(self, *args, **kwargs):
        return roundp(_parse_expr(self), *args, **kwargs)

    @wraps(sql_expr)
    def sql_expr(self, *args, **kwargs):
        return sql_expr(_parse_expr(self), *args, **kwargs)


# TODO: Enable?
# pl.expr.laktory = pl.Expr.laktory
