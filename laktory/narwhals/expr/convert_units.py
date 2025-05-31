from __future__ import annotations

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
    import polars as pl

    import laktory  # noqa: F401

    df = nw.from_native(pl.DataFrame({"x": [1.0]}))
    df = df.with_columns(
        y=pl.col("x").laktory.convert_units(input_unit="m", output_unit="ft")
    )
    print(df.to_native().glimpse(return_as_string=True))
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
    return units.convert(self._expr, input_unit, output_unit)
