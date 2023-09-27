from laktory.sql import py_to_sql


def test_sql():
    # String
    s = py_to_sql("test")
    assert s == "'test'"

    # Number
    s = py_to_sql(3)
    assert s == "3"

    # Boolean
    s = py_to_sql(False)
    assert s == "False"

    # Dict
    s = py_to_sql(
        {
            "name": "John",
            "surname": "Doe",
            "address": {"number": 0, "street": "Main"},
            "verified": True,
        }
    )
    assert (
        s
        == "named_struct('name', 'John', 'surname', 'Doe', 'address', named_struct('number', 0, 'street', 'Main'), 'verified', True)"
    )

    # List
    s = py_to_sql([1, "test", True])
    assert s == "ARRAY(1, 'test', True)"

    # List of dicts
    s = py_to_sql(
        [
            {"numbers": [1, 2, 3], "address": {"number": 0, "street": "Main"}},
            True,
        ]
    )
    assert (
        s
        == "ARRAY(named_struct('numbers', ARRAY(1, 2, 3), 'address', named_struct('number', 0, 'street', 'Main')), True)"
    )


if __name__ == "__main__":
    test_sql()
