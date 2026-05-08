import io

import pytest

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
        "sources": [
            {"name": "source_a", "url": "url_a"},
            {"name": "source_b", "url": "url_b"},
        ],
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
        "sources": [
            {"name": "source_a", "url": "url_a"},
            {"name": "source_b", "url": "url_b"},
        ],
    }


def test_missing_use_target():
    data = "value: !use does_not_exist.yaml\n"
    with pytest.raises(FileNotFoundError, match="!use target not found"):
        RecursiveLoader.load(io.StringIO(data))


def test_missing_update_target():
    data = "<<: !update does_not_exist.yaml\n"
    with pytest.raises(FileNotFoundError, match="!update target not found"):
        RecursiveLoader.load(io.StringIO(data))


def test_missing_extend_target():
    data = "- !extend does_not_exist.yaml\n"
    with pytest.raises(FileNotFoundError, match="!extend target not found"):
        RecursiveLoader.load(io.StringIO(data))


def test_circular_use_reference():
    filepath = paths.data / "yaml_loader" / "circular_a.yaml"
    with open(filepath, "r") as fp:
        with pytest.raises(ValueError, match="Circular !use reference"):
            RecursiveLoader.load(fp)


def test_circular_update_reference():
    filepath = paths.data / "yaml_loader" / "merge_circular_a.yaml"
    with open(filepath, "r") as fp:
        with pytest.raises(ValueError, match="Circular !update reference"):
            RecursiveLoader.load(fp)


def test_variables_stack_restored_on_error():
    # Even when construction fails mid-mapping, the variables stack must be
    # restored to its pre-mapping length.
    data = "variables:\n  env: dev\nvalue: !use does_not_exist.yaml\n"

    stream = io.StringIO(data)
    loader = RecursiveLoader(stream)
    initial_len = len(loader.variables)
    try:
        loader.get_single_data()
    except Exception:
        pass
    assert len(loader.variables) == initial_len


def test_malformed_yaml_syntax():
    data = "key: {unclosed\n"
    with pytest.raises(Exception) as exc_info:
        RecursiveLoader.load(io.StringIO(data))
    assert "yaml" in type(exc_info.value).__module__


def test_merge_key_in_quoted_string():
    # <<: inside a quoted string value must NOT be corrupted by preprocessing.
    # The old str.replace would turn it into __merge_here: inside the string.
    filepath = paths.data / "yaml_loader" / "merge_key_in_string.yaml"
    with open(filepath, "r") as fp:
        data = RecursiveLoader.load(fp)
    assert data["description"] == "supports <<: syntax for merging"
    assert data["normal_key"] == "value"
