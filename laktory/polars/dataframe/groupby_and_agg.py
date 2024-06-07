from __future__ import annotations
import polars as pl
from typing import TYPE_CHECKING

from laktory._logger import get_logger

if TYPE_CHECKING:
    from laktory.models.transformers.polarschainnode import PolarsChainNode


logger = get_logger(__name__)


def groupby_and_agg(
    df,
    groupby_columns: list[str] = None,
    agg_expressions: list[PolarsChainNode] = None,
) -> pl.DataFrame:
    """
    Apply a groupby and create aggregation columns.

    Parameters
    ----------
    df:
        DataFrame
    groupby_columns:
        List of column names to group by
    agg_expressions:
        List of columns defining the aggregations

    Examples
    --------
    ```py
    import laktory  # noqa: F401
    import polars as pl

    df0 = pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "price": [200.0, 205.0],
            "tstamp": ["2023-09-01", "2023-09-02"],
        }
    )

    df = df0.laktory.groupby_and_agg(
        groupby_columns=["symbol"],
        agg_expressions=[
            {
                "column": {"name": "mean_price"},
                "polars_func_name": "mean",
                "polars_func_args": ["price"],
            },
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
    from laktory.models.transformers.polarschainnode import PolarsChainNode

    # Parse inputs
    if agg_expressions is None:
        raise ValueError("`agg_expressions` must be specified")
    if groupby_columns is None:
        groupby_columns = []

    logger.info(
        f"Executing groupby ({groupby_columns}) with {agg_expressions}"
    )

    # Groupby arguments
    groupby = []

    for c in groupby_columns:
        groupby += [c]

    # Agg arguments
    aggs = []
    for expr in agg_expressions:
        if not isinstance(expr, PolarsChainNode):
            expr = PolarsChainNode(**expr)

        expr.column.type = "_any"
        aggs += [
            expr.execute(
                df=df,
                # udfs=udfs,
                return_col=True,
            ).alias(expr.column.name)
        ]

    print(aggs)

    return df.groupby(groupby).agg(*aggs)
