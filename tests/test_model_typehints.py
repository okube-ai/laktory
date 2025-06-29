from laktory.models import BaseModel
from laktory.typing import VariableType


class Price(BaseModel):
    close: float = None
    open: float = None


class MyModel(BaseModel):
    active: bool = None
    id: int = None
    ids: list[int] = None
    price: Price = None
    prices: list[Price] = None
    symbol: str = None
    prices_dict: dict[int, Price] = None


def test_bool():
    # native type
    m = MyModel(active=True)
    assert m.active

    # string
    m = MyModel(active="yes")
    assert m.active == "yes"

    # annotation
    ann = m.model_fields["active"].annotation
    assert ann == bool | VariableType


def test_int():
    # native type
    m = MyModel(id=5)
    assert m.id == 5

    # string
    m = MyModel(id="some_id")
    assert m.id == "some_id"

    # annotation
    ann = m.model_fields["id"].annotation
    assert ann == int | VariableType


def test_string():
    # native type
    m = MyModel(symbol="AAPL")
    assert m.symbol == "AAPL"

    # string
    m = MyModel(symbol="other")
    assert m.symbol == "other"

    # annotation
    ann = m.model_fields["symbol"].annotation
    assert ann == str | VariableType


def test_model():
    # native type
    m = MyModel(price={"open": 1.0, "close": 2.0})
    assert m.price.open == 1.0
    assert m.price.close == 2.0

    # string
    m = MyModel(symbol="some_prices")
    assert m.symbol == "some_prices"

    # annotation
    ann = m.model_fields["price"].annotation
    assert ann == Price | VariableType


def test_list():
    # native type
    m = MyModel(ids=[1, 2, 3])
    assert m.ids == [1, 2, 3]

    # string
    m = MyModel(ids="ids")
    assert m.ids == "ids"

    # list of string
    m = MyModel(ids=["id0", "id1", "id2"])
    assert m.ids == ["id0", "id1", "id2"]

    # annotation
    ann = m.model_fields["ids"].annotation
    assert ann == list[int | VariableType] | VariableType


def test_dict():
    d = {1: {"open": 1.0, "close": 2.0}, 2: {"open": 5.0, "close": 6.0}}

    # native type
    m = MyModel(prices_dict=d)
    assert m.prices_dict == {
        1: Price(open=1.0, close=2.0),
        2: Price(open=5.0, close=6.0),
    }

    # string
    m = MyModel(prices_dict="some_prices")
    assert m.prices_dict == "some_prices"

    # dict of strings
    m = MyModel(prices_dict={"id1": "price1", "id2": "price2"})
    assert m.prices_dict == {"id1": "price1", "id2": "price2"}

    # annotation
    ann = m.model_fields["prices_dict"].annotation
    assert ann == dict[int | VariableType, Price | VariableType] | VariableType
