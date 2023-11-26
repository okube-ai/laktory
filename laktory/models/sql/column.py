from pydantic import field_validator
from pydantic import model_validator
from typing import Any
from typing import Callable
from typing import Union

from laktory._logger import get_logger
from laktory.contants import SUPPORTED_TYPES
from laktory.models.base import BaseModel
from laktory.spark import Column as SparkColumn

logger = get_logger(__name__)


class SparkFuncArg(BaseModel):
    value: Any
    is_column: bool = None
    to_lit: bool = None

    @model_validator(mode="after")
    def is_column_default(self) -> Any:
        if self.to_lit is None and self.is_column is None:
            if isinstance(self.value, str):
                self.is_column = True

        if self.is_column is None:
            self.is_column = False

        if self.to_lit is None:
            self.to_lit = False

        if self.is_column and self.to_lit:
            raise ValueError("Only one of `to_column` or `to_lit` can be True")

        return self

    def to_spark(self):
        import pyspark.sql.functions as F

        v = self.value
        if self.to_lit:
            v = F.lit(v)
        else:
            v = F.expr(v)
        return v


class Column(BaseModel):
    catalog_name: Union[str, None] = None
    comment: Union[str, None] = None
    name: str
    pii: Union[bool, None] = None
    schema_name: Union[str, None] = None
    spark_func_args: list[Union[str, SparkFuncArg]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None] = None
    sql_expression: Union[str, None] = None
    table_name: Union[str, None] = None
    type: str = "string"
    unit: Union[str, None] = None

    @field_validator("spark_func_args")
    def parse_args(cls, args: list[Union[str, SparkFuncArg]]) -> list[SparkFuncArg]:
        _args = []
        for a in args:
            if isinstance(a, SparkFuncArg):
                pass
            elif isinstance(a, dict):
                a = SparkFuncArg(**a)
            else:
                a = SparkFuncArg(value=a)
            _args += [a]
        return _args

    @field_validator("spark_func_kwargs")
    def parse_kwargs(
        cls, kwargs: dict[str, Union[str, SparkFuncArg]]
    ) -> dict[str, SparkFuncArg]:
        _kwargs = {}
        for k, a in kwargs.items():
            if isinstance(a, SparkFuncArg):
                pass
            elif isinstance(a, dict):
                a = SparkFuncArg(**a)
            else:
                a = SparkFuncArg(value=a)
            _kwargs[k] = a
        return _kwargs

    @field_validator("type")
    def default_load_path(cls, v: str) -> str:
        if "<" in v:
            return v
        else:
            if v not in SUPPORTED_TYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_TYPES}"
                )
        return v

    # ----------------------------------------------------------------------- #
    # Computed fields                                                         #
    # ----------------------------------------------------------------------- #

    @property
    def parent_full_name(self) -> str:
        _id = ""
        if self.catalog_name:
            _id += self.catalog_name

        if self.schema_name:
            if _id == "":
                _id = self.schema_name
            else:
                _id += f".{self.schema_name}"

        if self.table_name:
            if _id == "":
                _id = self.table_name
            else:
                _id += f".{self.table_name}"

        return _id

    @property
    def full_name(self) -> str:
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    @property
    def database_name(self) -> str:
        return self.schema_name

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def to_spark(
        self,
        df,
        udfs: list[Callable[[...], SparkColumn]] = None,
        raise_exception: bool = True,
    ) -> SparkColumn:
        import pyspark.sql.functions as F
        from laktory.spark import functions as LF
        from laktory.spark.dataframe import has_column

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # From SQL expression
        if self.sql_expression:
            logger.info(f"   {self.name}[{self.type}] as `{self.sql_expression}`)")
            col = F.expr(self.sql_expression).alias(self.name)
            if self.type not in ["_any"]:
                col = col.cast(self.type)
            return col

        # From Spark Function
        func_name = self.spark_func_name
        if func_name is None:
            func_name = "coalesce"

        # Get from UDFs
        f = udfs.get(func_name, None)
        if f is None:
            # Get from built-in spark functions
            f = getattr(F, func_name, None)

            if f is None:
                # Get from laktory functions
                f = getattr(LF, func_name, None)

        if f is None:
            raise ValueError(f"Function {func_name} is not available")

        _args = self.spark_func_args
        _kwargs = self.spark_func_kwargs

        logger.info(f"   {self.name}[{self.type}] as {func_name}({_args}, {_kwargs})")

        # Build args
        args = []
        expected_cols = 0
        found_cols = 0
        for i, _arg in enumerate(_args):
            if df is not None:
                if _arg.is_column:
                    expected_cols += 1
                    if not has_column(df, _arg.value):
                        # When columns are not found, they are simply skipped and a
                        # warning is issued. Some functions, like `coalesce` might list
                        # multiple arguments, but don't expect all of them to be
                        # available
                        logger.warning(f"Column '{_arg.value}' not available")
                        continue
                    else:
                        found_cols += 1

            # TODO: Review if required
            # if _arg.value.startswith("data.") or func_name == "coalesce":
            #     pass
            # input_type = dict(df.dtypes)[input_col_name]
            # if input_type in ["double"]:
            #     # Some bronze NaN data will be converted to 0 if cast to int
            #     input_col = F.when(F.isnan(input_col_name), None).otherwise(F.col(input_col_name))
            # if self.type not in ["_any"] and func_name not in [
            #     "to_safe_timestamp"
            # ]:
            #     input_col = F.col(input_col_name).cast(self.type)

            args += [_arg.to_spark()]

        if expected_cols > 0 and found_cols == 0:
            if raise_exception:
                raise ValueError(
                    f"None of the inputs columns ({_args}) for {self.name} have been found"
                )
            else:
                logger.info("Input columns not available. Skipping")
                return None

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.to_spark()

        # Function call
        col = f(*args, **kwargs)

        # Type Casting
        if self.type not in ["_any"]:
            col = col.cast(self.type)

        return col
