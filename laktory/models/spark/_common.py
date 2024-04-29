from typing import Union
from pydantic import field_validator
from pydantic import ValidationError

from laktory.models.spark.sparkfuncarg import SparkFuncArg
from laktory.models.datasources.tabledatasource import TableDataSource


@field_validator("spark_func_args")
def parse_args(cls, args: list[Union[str, SparkFuncArg]]) -> list[SparkFuncArg]:
    _args = []
    for a in args:
        if not isinstance(a, SparkFuncArg):
            a = SparkFuncArg(value=a)
        _args += [a]
    return _args


@field_validator("spark_func_kwargs")
def parse_kwargs(
    cls, kwargs: dict[str, Union[str, SparkFuncArg]]
) -> dict[str, SparkFuncArg]:
    _kwargs = {}
    for k, a in kwargs.items():
        if not isinstance(a, SparkFuncArg):
            a = SparkFuncArg(value=a)
        _kwargs[k] = a
    return _kwargs
