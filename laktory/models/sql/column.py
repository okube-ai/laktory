from pydantic import field_validator
from pydantic import model_validator
from typing import Any
from typing import Callable
from typing import Union

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_TYPES
from laktory.models.basemodel import BaseModel
from laktory.spark import Column as SparkColumn
from laktory.spark import DataFrame

logger = get_logger(__name__)


class SparkFuncArg(BaseModel):
    """
    Spark function argument

    Attributes
    ----------
    value:
        Value of the argument
    is_column:
        If `True`, the value is also the name of a column
    to_lit:
        If `True`, the value is converted to a spark column using
        `F.lit({value})`
    to_expr:
        If `True`, the value is converted to a spark column using
        `F.expr({value})`
    """

    value: Any
    is_column: bool = None
    to_lit: bool = None
    to_expr: bool = None

    @model_validator(mode="after")
    def set_defaults(self) -> Any:
        """Default options"""

        # If only value is provided and value is a string, assume value is column
        if self.is_column is None and self.to_expr is None and self.to_lit is None:
            if isinstance(self.value, str):
                self.is_column = True

        if self.is_column:
            if self.to_lit:
                raise ValueError(
                    "If `to_column` is `True`, `to_lit` must be `None` or `False`"
                )

            if self.to_expr == False:
                raise ValueError(
                    "If `to_column` is `True`, `to_expr` must be `None` or `True`"
                )

            self.to_expr = True
            self.to_lit = False

        else:
            self.is_column = False

            if self.to_lit is None and self.to_expr is None:
                self.to_lit = True
                self.to_expr = False

            if self.to_lit is None:
                self.to_lit = False

            if self.to_expr is None:
                self.to_expr = False

        return self

    def to_spark(self):
        import pyspark.sql.functions as F

        v = self.value
        if self.to_lit:
            v = F.lit(v)
        elif self.to_expr:
            v = F.expr(v)
        return v


class Column(BaseModel):
    """
    Definition of a table column, including instructions on how to build the
    column using spark and an input dataframe.

    Attributes
    ----------
    catalog_name:
        Name of the catalog string the column table
    comment:
        Text description of the column
    name:
        Name of the column
    pii:
        If `True`, the column is flagged as Personally Identifiable Information
    schema_name:
        Name of the schema storing the column table
    spark_func_args:
        List of arguments to be passed to the spark function to build the
        column.
    spark_func_kwargs:
        List of keyword arguments to be passed to the spark function to build
        the column.
    spark_func_name:
        Name of the spark function to build the column. Mutually exclusive to
        `sql_expression`
    sql_expression:
        Expression defining how to build the column. Mutually exclusive to
        `spark_func_name`
    table_name:
        Name of the table storing the column.
    type:
        Column data type
    unit:
        Column units

    Examples
    --------
    ```py
    from laktory import models

    df = spark.createDataFrame(
        [
            [200.0],
            [202.0],
            [201.5],
        ],
        ["data.open"],
    )

    col = models.Column(
        name="open",
        spark_func_name="poly1",
        spark_func_args=[
            "data.open",
        ],
        spark_func_kwargs={"a": {"value": 2.0, "to_lit": True}},
        type="double",
    )

    spark_col = col.to_spark(df)
    ```
    """

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
        """Table full name `{catalog_name}.{schema_name}.{table_name}`"""
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
        """Column full name `{catalog_name}.{schema_name}.{table_name}.{column_name}`"""
        _id = self.name
        if self.parent_full_name is not None:
            _id = f"{self.parent_full_name}.{_id}"
        return _id

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def to_spark(
        self,
        df: DataFrame,
        udfs: list[Callable[[...], SparkColumn]] = None,
        raise_exception: bool = True,
    ) -> SparkColumn:
        """
        Build Spark Column

        Parameters
        ----------
        df:
            Input DataFrame
        udfs:
            User-defined functions
        raise_exception
            If `True`, raise exception when input columns are not available,
            else, skip.
        """
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
        for i, _arg in enumerate(_args):
            if df is not None:
                if _arg.is_column and not has_column(df, _arg.value):
                    logger.info(f"Column '{_arg.value}' not available")
                    if raise_exception:
                        raise ValueError(
                            f"Input column {_args} for {self.name} is not available"
                        )
                    else:
                        logger.info("Input columns not available. Skipping")
                        return None

            args += [_arg.to_spark()]

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


if __name__ == "__main__":
    from laktory import models

    col = models.Column(
        name="open",
        spark_func_name="poly1",
        spark_func_args=[
            "data.open",
        ],
        spark_func_kwargs={"a": {"value": 2.0, "to_lit": True}},
        type="double",
    )

    spark_col = col.to_spark(df)
