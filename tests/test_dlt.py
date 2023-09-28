import pytest
from laktory import dlt


def test_dlt():
    assert dlt.is_mocked()
    assert dlt.is_debug()

    assert hasattr(dlt, "table")
    assert hasattr(dlt, "read")
    assert hasattr(dlt, "read_stream")


if __name__ == "__main__":
    test_dlt()
