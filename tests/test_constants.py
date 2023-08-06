from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from laktory.contants import SUPPORTED_TYPES


def test_constants():
    assert SUPPORTED_TYPES["double"] == DoubleType()
    assert "str" not in SUPPORTED_TYPES
    assert SUPPORTED_TYPES["string"] == StringType()


if __name__ == "__main__":
    test_constants()
