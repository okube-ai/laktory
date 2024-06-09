import polars as pl
from planck import units

__all__ = [
    "convert_units",
]


# --------------------------------------------------------------------------- #
# Polynomials                                                                 #
# --------------------------------------------------------------------------- #


def convert_units(
    x: pl.Expr,
    input_unit: str,
    output_unit: str,
) -> pl.Expr:
    """
    Units conversion

    Parameters
    ----------
    x:
        Input column
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
    import laktory  # noqa: F401
    import polars as pl

    df = pl.DataFrame({"x": [1.0]})
    df = df.with_columns(
        y=pl.Expr.laktory.convert_units(pl.col("x"), input_unit="m", output_unit="ft")
    )
    print(df.glimpse(return_as_string=True))
    '''
    Rows: 1
    Columns: 2
    $ x <f64> 1.0
    $ y <f64> 3.280839895013124
    '''
    ```

    References
    ----------
    The units conversion function use [planck](https://www.okube.ai/planck/) convert as a backend.
    """
    return units.convert(x, input_unit, output_unit)
