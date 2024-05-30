import laktory
from laktory.models import BaseModel
from laktory._parsers import remove_empty
from laktory._parsers import merge_dicts


class Camel(BaseModel):
    d: dict = {}
    l: list = []


def test_camel_case():
    camel = Camel(
        d={"this_is_a_test": ["value_a", "value_b", {"key_alpha": 0, "keyBeta": 1}]},
        l=["a", "a-b-c", "class_member"],
    )
    d = laktory._parsers.camelize_keys(camel.model_dump())
    print(d)
    assert d == {
        "d": {"thisIsATest": ["value_a", "value_b", {"keyBeta": 1, "keyAlpha": 0}]},
        "l": ["a", "a-b-c", "class_member"],
    }


def test_remove_empty():
    d = {
        "a": "a",
        "b": {},
        "c": [{}, "c", []],
        "d": [],
    }

    assert remove_empty(d) == {"a": "a", "c": ["c"]}


def test_merge_dicts():

    baseline = {
        "name": "Apple",
        "symbols": {
            "nasdaq": "AAPL",
            "sp500": "APPL",
        },
        "prices": [
            {
                "timestamp": 0,
                "open": 1,
                "close": 2,
            },
            {
                "timestamp": 1,
                "open": 3,
                "close": 4,
            },
            {
                "timestamp": 2,
                "open": 5,
                "close": 6,
            },
        ],
    }

    overwrite = {
        "symbols": {
            "sp500": "AAPL",
        },
        "prices": {
            1: {
                "close": 5,
            }
        },
    }

    result = merge_dicts(baseline, overwrite)

    assert result == {
        "name": "Apple",
        "symbols": {"nasdaq": "AAPL", "sp500": "AAPL"},
        "prices": [
            {"timestamp": 0, "open": 1, "close": 2},
            {"timestamp": 1, "open": 3, "close": 5},
            {"timestamp": 2, "open": 5, "close": 6},
        ],
    }


if __name__ == "__main__":
    test_camel_case()
    test_remove_empty()
    test_merge_dicts()
