from typing import Any

import pytest

from laktory._testing import Paths
from laktory.models import BaseModel

paths = Paths(__file__)


class A(BaseModel):
    v0: float
    v1: float


class B(BaseModel):
    s0: str
    s1: str


class M(BaseModel):
    i: int
    values: list[float]
    a_models: list[A]
    b_models: list[B]


class M1(BaseModel):
    i: int
    d: dict[str, Any]
    b: B
    a_models: dict[str, A]
    b_models: list[B]


@pytest.fixture
def m():
    return M(
        i=1,
        values=[1, 2],
        a_models=[{"v0": 0.0, "v1": 1.0}],
        b_models=[{"s0": "0", "s1": "1"}],
    )


@pytest.fixture
def m1():
    return M1(
        i=1,
        d={"x": 0, "y": 1},
        b={"s0": "0", "s1": "1"},
        a_models={"0": {"v0": 0.0, "v1": 1.0}},
        b_models=[{"s0": "0", "s1": "1"}],
    )


def test_read_yaml():
    class Prices(BaseModel):
        open: float = None
        close: float = None

    class Stock(BaseModel):
        name: str = None
        symbol: str = None
        prices: Prices = None
        exchange: str = None
        fees: float = None
        rate: float = None

    class Stocks(BaseModel):
        stocks: list[Stock] = None
        query: str = None

    with open(paths.data / "yaml_loader" / "stocks_with_vars.yaml", "r") as fp:
        b = Stocks.model_validate_yaml(fp, vars={"symbol_amazon": "AMZN"})

    data = b.model_dump(exclude_unset=True)
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
                "prices": {"open": 1.0, "close": 2.0},
                "exchange": "nasdaq",
                "fees": 0.5,
                "rate": 0.1,
            },
        ],
        "query": "SELECT\n    *\nFORM\n    {df}\nWHERE\n-- COMMENT\n    SYMBOL = 'AAPL'\n;\n",
    }


def test_dump_yaml(m):
    print(m.model_dump_yaml())
    assert m.model_dump_yaml().startswith("a_models:")


def test_camelize(m):
    m._configure_serializer(camel=True)
    dump = m.model_dump()
    m._configure_serializer(camel=False)

    assert "bModels" in dump
    assert "v1" in dump["aModels"][0]


def test_singular(m):
    m._configure_serializer(singular=True)
    dump = m.model_dump()

    assert "values" in dump
    assert "a_model" in dump


def test_update(m1):
    # # Simple field
    m = m1.model_copy()
    m.update({"i": 2})
    assert m.i == 2

    # # Dict field
    m = m1.model_copy()
    m.update({"d": {"y": 2}})
    assert m.d == {"x": 0, "y": 2}

    # # Model field
    m = m1.model_copy()
    m.update({"b": {"s1": "2"}})
    assert m.b == B(s0="0", s1="2")

    # # Dict of models
    m = m1.model_copy()
    m.update({"a_models": {"0": {"v1": 2.0}}})
    assert m.a_models["0"] == A(v0=0.0, v1=2.0)

    # List of models
    m = m1.model_copy()
    m.update({"b_models": [{"s0": "00", "s1": "2"}]})
    assert m.b_models == [B(s0="00", s1="2")]
