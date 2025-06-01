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
    import polars as pl
    import narwhals as nw

    import laktory  # noqa: F401

    df0 = pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "price": [200.0, 205.0],
            "tstamp": ["2023-09-01", "2023-09-02"],
        }
    )
    df0 = nw.from_native(df0)

    df = df0.laktory.groupby_and_agg(
        groupby_columns=["symbol"],
        agg_expressions=[
            "nw.col('price').mean().alias('mean_price')",
        ],
    )

    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ symbol     <str> 'AAPL'
    $ mean_price <f64> 202.5
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
