from laktory import RecursiveLoader
from laktory._testing import Paths

paths = Paths(__file__)


def test_read():
    filepath = paths.data / "yaml_loader" / "main.yaml"

    with open(filepath, "r") as fp:
        data = RecursiveLoader.load(fp)

    assert data == {
        "stocks": [
            {"name": "microsoft", "symbol": "MSFT", "close": 175.0},
            {
                "name": "apple",
                "symbol": "AAPL",
                "close": 140.0,
                "first_traded": "june 1981",
                "open": 138,
                "high": 141,
                "low": 137,
            },
            "googl",
            "!append: /Users/osoucy/Documents/sources/okube/laktory/tests/data/yaml_loader/${vars.others}.yaml",
        ]
    }


if __name__ == "__main__":
    test_read()
