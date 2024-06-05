import json
import polars as pl


def schema_flat(self) -> list[str]:
    """
    Returns a flattened list of columns

    Parameters
    ----------
    df:
        Input DataFrame

    Returns
    -------
    :
        List of columns

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
    print(df.schema_flat())
    '''
    [
        'indexx',
        'stock',
        'stock.symbol',
        'stock.name',
        'prices',
        'prices[*].open',
        'prices[*].close',
    ]
    '''
    ```
    """

    df = self._df

    def get_fields(schema):
        field_names = []
        for f_name, f_type in schema.items():

            if isinstance(f_type, pl.Struct):
                _field_names = get_fields(dict(f_type))
                field_names += [f_name]
                field_names += [f"{f_name}.{v}" for v in _field_names]

            elif isinstance(f_type, pl.List):
                print(f_name, f_type, f_type.inner)
                field_names += [f_name]
                if isinstance(f_type.inner, pl.Struct):
                    _field_names = get_fields(dict(f_type.inner))
                    field_names += [f"{f_name}[*].{v}" for v in _field_names]

            else:

                field_names += [f_name]

        return field_names

    return get_fields(df.schema)
