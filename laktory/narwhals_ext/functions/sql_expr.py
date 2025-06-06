import narwhals as nw

from laktory.sqlparser import SQLParser


def sql_expr(sql: str) -> nw.Expr:
    """
    Parse SQL expression.

    Parameters
    ----------
    sql:
        SQL Expression

    Returns
    -------
    :
        Narwhals expression
    """
    parser = SQLParser()
    return parser.parse(sql)
