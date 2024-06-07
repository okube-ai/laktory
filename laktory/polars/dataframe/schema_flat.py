import polars as pl


def schema_flat(df: pl.DataFrame) -> list[str]:
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
    import polars as pl

    df = pl.DataFrame(
        {
            "indexx": [1, 2, 3],
            "stock": [
                {"symbol": "AAPL", "name": "Apple"},
                {"symbol": "MSFT", "name": "Microsoft"},
                {"symbol": "GOOGL", "name": "Google"},
            ],
            "prices": [
                [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
                [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
                [{"open": 1, "close": 2}, {"open": 1, "close": 2}],
            ],
        }
    )

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

    def get_fields(schema):
        field_names = []
        for f_name, f_type in schema.items():

            if isinstance(f_type, pl.Struct):
                _field_names = get_fields(dict(f_type))
                field_names += [f_name]
                field_names += [f"{f_name}.{v}" for v in _field_names]

            elif isinstance(f_type, pl.List):
                field_names += [f_name]
                if isinstance(f_type.inner, pl.Struct):
                    _field_names = get_fields(dict(f_type.inner))
                    field_names += [f"{f_name}[*].{v}" for v in _field_names]

            else:

                field_names += [f_name]

        return field_names

    return get_fields(df.schema)
