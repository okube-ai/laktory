import pyspark.sql.functions as F
from pyspark.sql.column import Column
from planck import units

from laktory.spark.functions._common import (
    COLUMN_OR_NAME,
    INT_OR_COLUMN,
    FLOAT_OR_COLUMN,
    STRING_OR_COLUMN,
    _col,
    _lit,
)

__all__ = [
    "convert_units",
]


# --------------------------------------------------------------------------- #
# Polynomials                                                                 #
# --------------------------------------------------------------------------- #


def convert_units(
    x: COLUMN_OR_NAME,
    input_unit: str,
    output_unit: str,
) -> Column:
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
    import pyspark.sql.functions as F

    df = spark.createDataFrame([[1.0]], ["x"])
    df = df.withColumn("y", F.laktory.convert_units("x", input_unit="m", output_unit="ft"))
    print(df.laktory.show_string())
    '''
    +---+-----------------+
    |  x|                y|
    +---+-----------------+
    |1.0|3.280839895013124|
    +---+-----------------+
    '''
    ```

    References
    ----------
    The units conversion function use [planck](https://www.okube.ai/planck/) convert as a backend.
    """
    return units.convert(_col(x), input_unit, output_unit)
