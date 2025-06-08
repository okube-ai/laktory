from functools import wraps

import narwhals as nw

from laktory.narwhals_ext.expr.convert_units import convert_units
from laktory.narwhals_ext.expr.roundp import roundp
from laktory.narwhals_ext.expr.string_split import string_split
from laktory.narwhals_ext.namespace import register_expr_namespace


@register_expr_namespace("laktory")
class LaktoryExpr:  # noqa: F811
    def __init__(self, expr: nw.Expr):
        self._expr = expr

    @wraps(convert_units)
    def convert_units(self, *args, **kwargs):
        return convert_units(self, *args, **kwargs)

    @wraps(string_split)
    def string_split(self, *args, **kwargs):
        return string_split(self, *args, **kwargs)

    @wraps(roundp)
    def roundp(self, *args, **kwargs):
        return roundp(self, *args, **kwargs)
