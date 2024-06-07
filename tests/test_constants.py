from laktory.constants import SUPPORTED_DATATYPES


def test_constants():
    assert "double" in SUPPORTED_DATATYPES
    assert "str" not in SUPPORTED_DATATYPES


if __name__ == "__main__":
    test_constants()
