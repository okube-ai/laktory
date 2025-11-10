from laktory._testing import Paths
from laktory.yaml import RecursiveLoader

paths = Paths(__file__)


def test_read():
    filepath = paths.data / "yaml_loader" / "stocks.yaml"

    with open(filepath, "r") as fp:
        data = RecursiveLoader.load(fp)

    assert data == {
        "stocks": [
            {
                "name": "apple",
                "symbol": "AAPL",
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "amazon",
                "symbol": "AMZN",
                "prices": {"open": 2.0, "close": 4.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "google",
                "symbol": "GOOGL",
                "prices": {"open": 5.0, "close": 6.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "microsoft",
                "symbol": "MSFT",
                "prices": {"open": 7.0, "close": 8.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
        ],
        "query": "SELECT\n    *\nFORM\n    {df}\nWHERE\n-- COMMENT\n    SYMBOL = 'AAPL'\n;\n",
    }


def test_read_with_variables():
    filepath = paths.data / "yaml_loader" / "stocks_with_vars.yaml"

    with open(filepath, "r") as fp:
        data = RecursiveLoader.load(fp, vars={"symbol_amazon": "amzn"})

    assert data == {
        "stocks": [
            {
                "name": "apple",
                "symbol": "AAPL",
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
            {
                "name": "amazon",
                "symbol": "AMZN",
                "prices": {"open": 2.0, "close": 4.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
                "variables": {"symbol": "amzn"},
            },
            {
                "name": "google",
                "symbol": "GOOGL",
                "prices": {"open": 5.0, "close": 6.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
                "variables": {"symbol": "googl"},
            },
            {
                "name": "microsoft",
                "symbol": "MSFT",
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
        ],
        "variables": {"symbol": "aapl"},
        "query": "SELECT\n    *\nFORM\n    {df}\nWHERE\n-- COMMENT\n    SYMBOL = 'AAPL'\n;\n",
    }
