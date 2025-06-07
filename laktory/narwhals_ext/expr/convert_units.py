import narwhals as nw
from planck import units


def convert_units(
    self,
    input_unit: str,
    output_unit: str,
) -> nw.Expr:
    """
    Units conversion

    Parameters
    ----------
    input_unit:
        Input units
    output_unit:
        Output units

    Returns
    -------
    :
        Output column

    Examples
    --------
    ```py
    import narwhals as nw
    import polars as pl

    import laktory as lk  # noqa: F401

    df = nw.from_native(pl.DataFrame({"x": [1]}))
    df = df.with_columns(
        y=nw.col("x").laktory.convert_units(input_unit="m", output_unit="ft")
    )
    print(df)
    '''
    ┌──────────────────┐
    |Narwhals DataFrame|
    |------------------|
    | | x | y       |  |
    | |---|---------|  |
    | | 1 | 3.28084 |  |
    └──────────────────┘
    '''
    ```

    References
    ----------
    The units conversion function use [planck](https://www.okube.ai/planck/) convert as a backend.
    """
    return units.convert(self._expr, input_unit, output_unit)
