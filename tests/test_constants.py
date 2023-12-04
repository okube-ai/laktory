from laktory.constants import SUPPORTED_TYPES


def test_constants():
    assert "double" in SUPPORTED_TYPES
    assert "str" not in SUPPORTED_TYPES


if __name__ == "__main__":
    test_constants()
