from medaillon.models import Column


def test_read():
    with open("./airspeed.yaml", "r") as fp:
        c0 = Column.model_validate_yaml(fp)

    with open("./airspeed.json", "r") as fp:
        c1 = Column.model_validate_json_file(fp)

    assert c0 == c1

    return c0


if __name__ == "__main__":
    column = test_read()
