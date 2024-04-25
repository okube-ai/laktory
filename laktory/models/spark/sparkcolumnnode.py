from pydantic import field_validator
from typing import Any
from typing import Union
from typing import Callable

from laktory._logger import get_logger
from laktory.constants import SUPPORTED_TYPES
from laktory.models.basemodel import BaseModel
from laktory.models.spark.sparkfuncarg import SparkFuncArg
from laktory.spark import DataFrame
from laktory.spark import Column as SparkColumn
from laktory.models.spark._common import parse_args
from laktory.models.spark._common import parse_kwargs

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main Class                                                                  #
# --------------------------------------------------------------------------- #

class SparkColumnNode(BaseModel):
    name: str
    raise_missing_arg_exception: Union[bool, None] = True
    type: str = "string"
    unit: Union[str, None] = None
    spark_func_args: list[Union[str, SparkFuncArg, Any]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None] = None
    sql_expression: Union[str, None] = None

    parse_args = parse_args
    parse_kwargs = parse_kwargs

    @field_validator("type")
    def check_type(cls, v: str) -> str:
        if "<" in v:
            return v
        else:
            if v not in SUPPORTED_TYPES:
                raise ValueError(
                    f"Type {v} is not supported. Select one of {SUPPORTED_TYPES}"
                )
        return v

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
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
            # if df is not None:
            #     if _arg.is_column and not has_column(df, _arg.value):
            #         logger.info(f"Column '{_arg.value}' not available")
            #
            #         if self.raise_missing_arg_exception:
            #             if raise_exception:
            #                 raise ValueError(
            #                     f"Input column {_arg.value} missing. Abort building {self.name}."
            #                 )
            #             else:
            #                 logger.info(
            #                     f"Input column {_arg.value} missing. Skip building {self.name}"
            #                 )
            #                 return None
            #         else:
            #             continue

            args += [_arg.eval()]

        if len(_args) > 0 and len(args) < 1:
            if raise_exception:
                raise ValueError(
                    f"All input columns are  missing. Abort building {self.name}"
                )
            else:
                logger.info(f"All input columns are missing. Skip building {self.name}")
                return None

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.to_spark()

        # Function call
        logger.info(f"   {self.name}[{self.type}] as {func_name}({args}, {kwargs})")
        col = f(*args, **kwargs)

        # Type Casting
        if self.type not in ["_any"]:
            col = col.cast(self.type)

        return col

