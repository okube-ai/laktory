import pytest


def test_dlt():
    with pytest.raises(ModuleNotFoundError):
        from laktory import dlt


if __name__ == "__main__":
    test_dlt()
