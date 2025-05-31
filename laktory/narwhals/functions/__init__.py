from __future__ import annotations

from functools import wraps

import narwhals as nw

from laktory.narwhals.functions.current_timestamp import current_timestamp
from laktory.narwhals.functions.sql_expr import sql_expr


class LaktoryFuncs:  # noqa: F811
    @wraps(current_timestamp)
    def current_timestamp(*args, **kwargs):
        return current_timestamp(*args, **kwargs)

    @wraps(sql_expr)
    def sql_expr(*args, **kwargs):
        return sql_expr(*args, **kwargs)


nw.laktory = LaktoryFuncs
