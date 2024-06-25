from laktory.constants import SUPPORTED_DATATYPES
from laktory.spark import DATATYPES_MAP as SPARK_DATATYPES_MAP
from laktory.polars import DATATYPES_MAP as POLARS_DATATYPES_MAP


def test_spark_datatypes():

    for dtype in SUPPORTED_DATATYPES:
        assert dtype in SPARK_DATATYPES_MAP

    for k, v in SPARK_DATATYPES_MAP.items():
        assert k in SUPPORTED_DATATYPES
        assert v is not None


def test_polars_datatypes():

    for dtype in SUPPORTED_DATATYPES:
        assert dtype in POLARS_DATATYPES_MAP

    for k, v in POLARS_DATATYPES_MAP.items():
        assert k in SUPPORTED_DATATYPES
        assert v is not None


if __name__ == "__main__":
    test_spark_datatypes()
    test_polars_datatypes()
