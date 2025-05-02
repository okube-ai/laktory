from laktory.sqlparser import SQLParser


def sql_expr(sql_expr):
    parser = SQLParser()
    return parser.parse(sql_expr)


# nw.sql_expr = sql_expr  # TODO: Review if we want to ducktype Narwhals
