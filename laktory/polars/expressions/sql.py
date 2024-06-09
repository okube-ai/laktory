import polars as pl
import sqlparse
from sqlparse.sql import Comparison
from sqlparse.tokens import Keyword


def _parse_token(token):

    # Numerical
    try:
        _ = float(token)
        if "." in token:
            return _
        else:
            return int(token)
    except (ValueError, TypeError):
        pass

    # String constant
    if token.startswith("'") or token.startswith('"'):
        return token[1:-1]

    # Structure
    if "." in token:
        names = token.split(".")
        col = pl.col(names[0])
        for n in names[1:]:
            col = col.struct.field(n)
        return col

    # Column
    return pl.col(token)


def _parse_compare(condition: str):

    # Remove whitespaces
    condition = condition.replace(" ", "")

    # Breakdown
    parsed = sqlparse.parse(condition)[0]
    comparison = [token for token in parsed.tokens if isinstance(token, Comparison)][0]
    left = comparison.left.value
    operator = comparison.tokens[1].value
    right = comparison.right.value

    # Build expression
    if operator == "<":
        return _parse_token(left) < _parse_token(right)
    elif operator == ">":
        return _parse_token(left) > _parse_token(right)
    elif operator == "<=":
        return _parse_token(left) <= _parse_token(right)
    elif operator == ">=":
        return _parse_token(left) >= _parse_token(right)
    elif operator == "==":
        return _parse_token(left) == _parse_token(right)
    elif operator == "!=":
        return _parse_token(left) != _parse_token(right)

    return condition


def sql_expr(sql: str) -> pl.Expr:
    """
    Parse SQL expression to polars expression(s) with support for nested
    structure

    Parameters
    ----------
    sql :
        SQL expression

    Returns
    -------
    :
        Polar expression

    ```py
    import laktory  # noqa: F401
    import polars as pl

    exp = pl.Expr.laktory.sql_expr("data.close")
    print(exp)
    #> col("data").struct.field_by_name(close)()
    exp = pl.Expr.laktory.sql_expr("data.close > 5.0")
    print(exp)
    #> [(col("data").struct.field_by_name(close)()) > (dyn float: 5.0)]
    ```
    """
    try:
        return pl.sql_expr(sql)

    except pl.exceptions.ComputeError:
        parsed = sqlparse.parse(sql)[0]
        expressions = []

        for token in parsed.tokens:
            if isinstance(token, Comparison):
                expressions += [_parse_compare(str(token))]
            elif token.ttype is Keyword and token.value.upper() in ["AND", "OR"]:
                expressions.append(token.value.upper())
            elif str(token).replace(" ", "") == "":
                pass
            else:
                expressions += [_parse_token(str(token))]

        expr = expressions[0]
        i = 1
        while i < len(expressions) - 1:
            if expressions[i] == "AND":
                expr = expr & expressions[i + 1]
            elif expressions[i] == "OR":
                expr = expr | expressions[i + 1]
            i += 2

        return expr
