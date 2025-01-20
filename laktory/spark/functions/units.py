# import pyspark.sql.functions as F #noqa
from planck import units
from pyspark.sql.column import Column

from laktory.spark.functions._common import COLUMN_OR_NAME
from laktory.spark.functions._common import _col

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
    import pyspark.sql.functions as F

    import laktory  # noqa: F401

    df = spark.createDataFrame([[1.0]], ["x"])
    df = df.withColumn(
        "y", F.laktory.convert_units("x", input_unit="m", output_unit="ft")
    )
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


if __name__ == "__main__":
    import pyspark.sql.functions as F

    import laktory  # noqa: F401
    from laktory._testing import sparkf

    spark = sparkf.spark

    df = spark.createDataFrame([[1.0]], ["x"])
    df = df.withColumn(
        "y", F.laktory.convert_units("x", input_unit="m", output_unit="ft")
    )
    print(df.laktory.show_string())
