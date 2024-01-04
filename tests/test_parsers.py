import laktory
from laktory.models import BaseModel


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


if __name__ == "__main__":
    test_camel_case()
