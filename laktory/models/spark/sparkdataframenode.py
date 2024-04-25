from pydantic import field_validator
from typing import Union
from typing import Any
from typing import Callable

from laktory._logger import get_logger
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

class SparkDataFrameNode(BaseModel):
    spark_func_args: list[Union[Any, SparkFuncArg]] = []
    spark_func_kwargs: dict[str, Union[Any, SparkFuncArg]] = {}
    spark_func_name: Union[str, None] = None
    sql_expression: Union[str, None] = None

    parse_args = parse_args
    parse_kwargs = parse_kwargs

    # ----------------------------------------------------------------------- #
    # Class Methods                                                           #
    # ----------------------------------------------------------------------- #

    def execute(
        self,
        df: DataFrame,
        udfs: list[Callable[[...], SparkColumn]] = None,
        raise_exception: bool = True,
        spark = None,
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
        from pyspark.sql import DataFrame
        from laktory.spark.dataframe import has_column
        from laktory.spark import DataFrame as LaktoryDataFrame

        if udfs is None:
            udfs = []
        udfs = {f.__name__: f for f in udfs}

        # From SQL expression
        if self.sql_expression:
            raise NotImplementedError("sql_expression not supported yet")

            df.createOrReplaceTempView("_df")
            df = spark.sql(self.sql_expression)
            return df

        # From Spark Function
        func_name = self.spark_func_name
        if self.spark_func_name is None:
            raise ValueError("`spark_func_name` must be specific for a spark dataframe node")

        # Get from UDFs
        f = udfs.get(func_name, None)
        if f is None:
            # Get from built-in spark functions
            f = getattr(DataFrame, func_name, None)

            if f is None:
                # Get from laktory functions
                f = getattr(LaktoryDataFrame, func_name, None)

        if f is None:
            raise ValueError(f"Function {func_name} is not available")

        _args = self.spark_func_args
        _kwargs = self.spark_func_kwargs

        logger.info(f"   {func_name}({_args}, {_kwargs})")

        # Build args
        args = []
        for i, _arg in enumerate(_args):
            # if _arg.is_column and not has_column(df, _arg.value):
            #     logger.info(f"Column '{_arg.value}' not available")

                # if self.raise_missing_arg_exception:
                #     if raise_exception:
                #         raise ValueError(
                #             f"Input column {_arg.value} missing. Abort building {self.name}."
                #         )
                #     else:
                #         logger.info(
                #             f"Input column {_arg.value} missing. Skip building {self.name}"
                #         )
                #         return None
                # else:
                #     continue

            args += [_arg.eval(spark)]

        # if len(_args) > 0 and len(args) < 1:
        #     if raise_exception:
        #         raise ValueError(
        #             f"All input columns are  missing. Abort building {self.name}"
        #         )
        #     else:
        #         logger.info(f"All input columns are missing. Skip building {self.name}")
        #         return None

        # Build kwargs
        kwargs = {}
        for k, _arg in _kwargs.items():
            kwargs[k] = _arg.to_spark()

        # Function call
        logger.info(f"   {func_name}({args}, {kwargs})")
        df = f(df, *args, **kwargs)

        return df

