import polars as pl


def union(self, other: pl.DataFrame) -> pl.DataFrame:
    """
    """

    df = self._df
    return pl.concat([df, other])

