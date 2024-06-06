import json
from pyspark.sql.dataframe import DataFrame


def schema_flat(df: DataFrame) -> list[str]:
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
    print(df.laktory.schema_flat())
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

    def get_fields(json_schema):
        field_names = []
        for f in json_schema.get("fields", []):
            f_name = f["name"]
            f_type = f["type"]
            if isinstance(f_type, dict):
                if f_type["type"] == "array":
                    e_type = f_type["elementType"]
                    field_names += [f_name]
                    if isinstance(e_type, dict):
                        _field_names = get_fields(f_type["elementType"])
                        field_names += [f"{f_name}[*].{v}" for v in _field_names]
                elif f_type["type"] == "struct":
                    _field_names = get_fields(f["type"])
                    field_names += [f_name]
                    field_names += [f"{f_name}.{v}" for v in _field_names]
                elif f_type["type"] == "map":
                    field_names += [f_name]
                else:
                    raise ValueError(f_type["type"])
            else:
                field_names += [f_name]
        return field_names

    return get_fields(json.loads(df.schema.json()))
