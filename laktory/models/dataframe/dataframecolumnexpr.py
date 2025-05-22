from __future__ import annotations

import re
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal

import narwhals as nw
from pydantic import Field
from pydantic import model_validator

from laktory._logger import get_logger
from laktory.enums import DataFrameBackends
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipelinechild import PipelineChild

if TYPE_CHECKING:
    import polars as pl
    import pyspark.sql.functions as F


logger = get_logger(__name__)

if TYPE_CHECKING:
    import polars as pl
    import pyspark.sql.functions as F


class DataFrameColumnExpr(BaseModel, PipelineChild):
    """
    DataFrame Column Expression defined with a string representation of DataFrame
    API expression or a SQL statement.

    Examples
    --------
    ```py
    from laktory import models

    df = pl.DataFrame(
        {
            "x": [1, 2, 3],
        }
    )

    expr1 = models.DataFrameColumnExpr(
        value="col('x')+lit(1)",
        dataframe_backend="POLARS",
        dataframe_api="NATIVE",
    )

    expr2 = models.DataFrameColumnExpr(
        value="x**2 + 1",
        type="SQL",
        dataframe_backend="POLARS",
        dataframe_api="NATIVE",
    )


    df = df.with_columns(y1=expr1.df_expr, y2=expr2.df_expr)

    print(df)
    ```
    """

    expr: str = Field(..., description="Expression string representation")
    type: Literal["SQL", "DF"] = Field(
        None,
        description="Expression type: DF or SQL. If `None` is specified, type is "
        "guessed from provided expression.",
    )

    @model_validator(mode="after")
    def guess_type(self) -> Any:
        if self.type:
            return self

        expr_clean = self.expr.strip().replace("\n", " ")

        type = "SQL"
        if re.findall(r"\w+\.\w+\(", expr_clean):
            type = "DF"

        for k in [
            "lit(",
            "col(",
            "F.",
            "nw.",
            "pl.",
        ]:
            if k in expr_clean:
                type = "DF"
                break

        self.type = type

        return self

    # ----------------------------------------------------------------------- #
    # Expressions                                                             #
    # ----------------------------------------------------------------------- #

    def to_sql_expr(self) -> str:
        """Column expression expressed as a SQL Statement"""
        # -> pure SQL

        if self.type == "SQL":
            return self.expr
        else:
            # TODO: Use SQLFrame?
            raise ValueError("DataFrame expression can't be converted to SQL")

    def to_expr(self) -> nw.Expr | "pl.Expr" | "F.Column":
        """Column expression expressed as DataFrame API object"""

        # # Adding udfs to global variables
        # if udfs is None:
        #     udfs = {}
        # for k, v in udfs.items():
        #     globals()[k] = v

        _value = self.expr.replace("\n", " ")

        if self.df_api == "NARWHALS":
            if self.type == "SQL":
                from laktory.narwhals.expr import sql_expr

                expr = sql_expr(_value)
            else:
                # Imports required to evaluate expressions
                import narwhals as nw  # noqa: F401
                from narwhals import col  # noqa: F401
                from narwhals import lit  # noqa: F401

                expr = eval(_value)
        else:
            if self.df_backend == DataFrameBackends.PYSPARK:
                if self.type == "SQL":
                    import pyspark.sql.functions as F

                    expr = F.expr(_value)
                else:
                    # Imports required to evaluate expressions
                    import pyspark.sql.functions as F  # noqa: F401
                    import pyspark.sql.types as T  # noqa: F401
                    from pyspark.sql.functions import col  # noqa: F401
                    from pyspark.sql.functions import lit  # noqa: F401

                    expr = eval(_value)

            elif self.df_backend == DataFrameBackends.POLARS:
                if self.type == "SQL":
                    import polars as pl

                    expr = pl.sql_expr(_value)
                else:
                    # Imports required to evaluate expressions
                    import polars as pl  # noqa: F401
                    import polars.functions as F  # noqa: F401
                    from polars import col  # noqa: F401
                    from polars import lit  # noqa: F401

                    expr = eval(self.expr)

            else:
                raise ValueError(
                    f"`dataframe_backend` '{self.df_backend}' is not supported."
                )
        #
        # # Cleaning up global variables
        # for k, v in udfs.items():
        #     del globals()[k]

        return expr

    #
    #
    #
    # def parsed_expr(self) -> list[str]:
    #     raise NotImplementedError()
    #     # from laktory.models.datasources.pipelinenodedatasource import (
    #     #     PipelineNodeDataSource,
    #     # )
    #     # from laktory.models.datasources.tabledatasource import TableDataSource
    #
    #     expr = self.sql_expr
    #
    #     expr = expr.replace("{df}", "df")
    #     pattern = r"\{nodes\.(.*?)\}"
    #     matches = re.findall(pattern, expr)
    #     for m in matches:
    #         expr = expr.replace("{nodes." + m + "}", f"nodes__{m}")
    #
    #     return expr.split(";")

    # @property
    # def upstream_node_names(self) -> list[str]:
    #     if self.sql_expr is None:
    #         return []
    #
    #     names = []
    #
    #     pattern = r"\{nodes\.(.*?)\}"
    #     matches = re.findall(pattern, self.sql_expr)
    #     for m in matches:
    #         names += [m]
    #
    #     return names
    #
    # @property
    # def data_sources(self):
    #     """Get all sources required by SQL Expression"""
    #     if self._data_sources is None:
    #         from laktory.models.datasources.pipelinenodedatasource import (
    #             PipelineNodeDataSource,
    #         )
    #
    #         sources = []
    #
    #         pattern = r"\{nodes\.(.*?)\}"
    #         matches = re.findall(pattern, self.sql_expr)
    #         for m in matches:
    #             sources += [PipelineNodeDataSource(node_name=m)]
    #
    #         self._data_sources = sources
    #
    #     return self._data_sources
    #
    # def execute(
    #     self,
    #     df: AnyFrame,
    #     # udfs: list[Callable[[...], Union[PolarsExpr, PolarsDataFrame]]] = None,
    #     **named_dfs: dict[str, AnyFrame],
    # ) -> Union[AnyFrame]:
    #     """
    #     Execute SQL expression on provided DataFrame `df`.
    #
    #     Parameters
    #     ----------
    #     df:
    #         Input dataframe
    #     udfs:
    #         User-defined functions
    #     named_dfs:
    #         Other DataFrame(s) to be passed to the method.
    #
    #     Returns
    #     -------
    #         Output dataframe
    #     """
    #
    #     # Get Backend
    #     backend = DataFrameBackends.from_df(df)
    #     #
    #     # from laktory.polars.datatypes import DATATYPES_MAP
    #     #
    #     # if udfs is None:
    #     #     udfs = []
    #     # udfs = {f.__name__: f for f in udfs}
    #     #
    #
    #     # From SQL expression
    #     logger.info(f"DataFrame as \n{self.sql_expr.strip()}")
    #     dfs = {"df": df}
    #     for k, v in named_dfs.items():
    #         dfs[k] = v
    #
    #     dfs = {k: v.to_native() for k, v in dfs.items()}
    #     df0 = list(dfs.values())[0]
    #
    #     if backend == DataFrameBackends.POLARS:
    #         import polars as pl
    #
    #         #
    #         # kwargs = {"df": df}
    #         # for source in self.data_sources:
    #         #     kwargs[f"nodes__{source.node.name}"] = source.read()
    #         # return pl.SQLContext(frames=dfs).execute(";".join(self.parsed_expr()))
    #         return pl.SQLContext(frames=dfs).execute(self.sql_expr)
    #
    #     elif backend == DataFrameBackends.PYSPARK:
    #         _spark = df0.sparkSession
    #
    #         # # Get pipeline node if executed from pipeline
    #         # pipeline_node = self.parent_pipeline_node
    #         #
    #         # # Set df id (to avoid temp view with conflicting names)
    #         # df_id = "df"
    #         # if pipeline_node:
    #         #     df_id = f"df_{pipeline_node.name}"
    #
    #         # df.createOrReplaceTempView(df_id)
    #         # for source in self.data_sources:
    #         #     _df = source.read(spark=_spark)
    #         #     _df.createOrReplaceTempView(f"nodes__{source.node.name}")
    #
    #         # Create views
    #         for k, _df in dfs.items():
    #             _df.createOrReplaceTempView(k)
    #
    #         # Run query
    #         _df = None
    #         for expr in self.sql_expr.split(";"):
    #             # for expr in self.parsed_expr():
    #             if expr.replace("\n", " ").strip() == "":
    #                 continue
    #             # _df = _spark.laktory.sql(expr)
    #             _df = _spark.sql(expr)
    #         if _df is None:
    #             raise ValueError(f"SQL Expression '{self.sql_expr}' is invalid")
    #         return _df
    #
    #     else:
    #         raise NotImplementedError(f"Backend '{backend}' is not supported.")
