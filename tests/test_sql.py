from laktory.sql import value_to_statement


def test_sql():

    # String
    s = value_to_statement("test")
    assert s == "'test'"

    # Number
    s = value_to_statement(3)
    assert s == "3"

    # Boolean
    s = value_to_statement(False)
    assert s == "False"

    # Dict
    s = value_to_statement({
        "name": "John",
        "surname": "Doe",
        "address": {
            "number": 0,
            "street": "Main"
        },
        "verified": True,
    })
    assert s == "named_struct('name', 'John', 'surname', 'Doe', 'address', named_struct('number', 0, 'street', 'Main'), 'verified', True)"

    # List
    s = value_to_statement([1, "test", True])
    assert s == "array(1, 'test', True)"

    # List of dicts
    s = value_to_statement([
        {
            "numbers": [1, 2, 3],
            "address": {
                "number": 0,
                "street": "Main"
            }
        },
        True,
        ])
    assert s == "array(named_struct('numbers', array(1, 2, 3), 'address', named_struct('number', 0, 'street', 'Main')), True)"


if __name__ == "__main__":
    test_sql()
