import re


def has_column(self, col: str) -> bool:
    """
    Check if column `col` exists in `df`

    Parameters
    ----------
    df:
        Input DataFrame
    col
        Column name

    Returns
    -------
    :
        Result

    Examples
    --------

    ```py
    import laktory  # noqa: F401
    import pyspark.sql.types as T

    schema = T.StructType(
        [
            T.StructField("indexx", T.IntegerType()),
            T.StructField(
                "stock",
                T.StructType(
                    [
                        T.StructField("symbol", T.StringType()),
                        T.StructField("name", T.StringType()),
                    ]
                ),
            ),
            T.StructField(
                "prices",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("open", T.IntegerType()),
                            T.StructField("close", T.IntegerType()),
                        ]
                    )
                ),
            ),
        ]
    )

    data = [
        (
            1,
            {"symbol": "AAPL", "name": "Apple"},
            [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
        ),
        (
            2,
            {"symbol": "MSFT", "name": "Microsoft"},
            [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
        ),
        (
            3,
            {"symbol": "GOOGL", "name": "Google"},
            [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
        ),
    ]

    df = spark.createDataFrame(data, schema=schema)
    print(df.has_column("symbol"))
    #> False
    print(df.has_column("`stock`.`symbol`"))
    #> True
    print(df.has_column("`prices[2]`.`close`"))
    #> True
    ```
    """

    df = self._df
    _col = re.sub(r"\[(\d+)\]", r"[*]", col)
    _col = re.sub(r"`", "", _col)
    return _col in df.laktory.schema_flat()
