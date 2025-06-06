import narwhals as nw


def schema_flat(self) -> list[str]:
    """
    Returns a flattened list of columns

    Returns
    -------
    :
        List of columns

    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df = nw.from_native(
        pl.DataFrame(
            {
                "index": [1, 2, 3],
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
    )

    print(df.laktory.schema_flat())
    '''
    [
        'index',
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
            if isinstance(f_type, nw.Struct):
                _field_names = get_fields(dict(f_type))
                field_names += [f_name]
                field_names += [f"{f_name}.{v}" for v in _field_names]

            elif isinstance(f_type, nw.List):
                field_names += [f_name]
                if isinstance(f_type.inner, nw.Struct):
                    _field_names = get_fields(dict(f_type.inner))
                    field_names += [f"{f_name}[*].{v}" for v in _field_names]

            else:
                field_names += [f_name]

        return field_names

    return get_fields(self._df.schema)
