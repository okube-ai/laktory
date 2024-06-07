import polars as pl


def union(df: pl.DataFrame, other: pl.DataFrame) -> pl.DataFrame:
    """ """
    return pl.concat([df, other])
