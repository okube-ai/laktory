import narwhals as nw

from laktory._logger import get_logger
from laktory.models.dataframe.dataframecolumnexpr import DataFrameColumnExpr
from laktory.typing import AnyFrame

logger = get_logger(__name__)


def groupby_and_agg(
    self,
    groupby_columns: list[str] = None,
    agg_expressions: list[DataFrameColumnExpr | str | nw.Expr] = None,
) -> AnyFrame:
    """
    Apply a groupby and create aggregation columns.

    Parameters
    ----------
    groupby_columns:
        List of column names to group by
    agg_expressions:
        List of columns defining the aggregations

    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df0 = nw.from_native(
        pl.DataFrame(
            {
                "x": [0, 0, 1, 1],
                "y": [1, 2, 3, 4],
            }
        )
    )

    df = df0.laktory.groupby_and_agg(
        groupby_columns=["x"],
        agg_expressions=[
            "nw.col('y').mean().alias('mean_price')",
        ],
    )

    print(df.sort("x"))
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    || x | mean_price ||
    ||---|------------||
    || 0 | 1.5        ||
    || 1 | 3.5        ||
    └──────────────────┘
    '''
    ```
    """
    from laktory.models.dataframe.dataframecolumnexpr import DataFrameColumnExpr

    # Parse inputs
    if agg_expressions is None:
        raise ValueError("`agg_expressions` must be specified")
    if groupby_columns is None:
        groupby_columns = []

    logger.info(f"Executing groupby ({groupby_columns}) with {agg_expressions}")

    # Groupby arguments
    groupby = []

    for c in groupby_columns:
        groupby += [c]

    # Agg arguments
    aggs = []
    for expr in agg_expressions:
        if isinstance(expr, str):
            expr = DataFrameColumnExpr(expr=expr).to_expr()

        elif isinstance(expr, dict):
            expr = DataFrameColumnExpr(**expr).to_expr()

        aggs += [expr]

    return self._df.group_by(groupby).agg(*aggs)
