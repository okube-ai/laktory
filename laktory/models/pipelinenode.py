# import os
# from datetime import datetime
# from zoneinfo import ZoneInfo
# from typing import Any
from typing import Literal
from typing import Union
#
# from pydantic import Field
# from pydantic import ConfigDict
#
from laktory.models.basemodel import BaseModel
from laktory.models.datasources.filedatasource import FileDataSource
from laktory.models.datasources.memorydatasource import MemoryDataSource
from laktory.models.datasources.tabledatasource import TableDataSource
from laktory.models.datasinks.filedatasink import FileDataSink
from laktory.models.datasinks.tabledatasink import TableDataSink
from laktory.models.pipelinenodeexpectation import PipelineNodeExpectation
from laktory.models.spark.sparkchain import SparkChain
from laktory.models.spark.sparkchainnode import SparkChainNode
from laktory._logger import get_logger

logger = get_logger(__name__)


class PipelineNode(BaseModel):
    """

    Attributes
    ----------

    Examples
    --------
    ```py

    ```
    """

    add_laktory_columns: bool = True
    drop_duplicates: Union[bool, list[str], None] = False
    drop_source_columns: Union[bool, None] = False
    chain: Union[SparkChain, None] = None
    expectations: list[PipelineNodeExpectation] = []
    id: str
    layer: Literal["BRONZE", "SILVER", "GOLD"] = None
    # primary_key
    # timestamp_key
    sink: Union[FileDataSink, TableDataSink, None] = None
    source: Union[FileDataSource, TableDataSource, MemoryDataSource, "PipelineNode", None] = None

    # def model_post_init(self, __context):
    #     super().model_post_init(__context)
    #
    #     # Add metadata
    #     if self.data is not None:
    #         self.data["_name"] = self.name
    #         self.data["_producer_name"] = self.producer.name
    #         tstamp = self.data.get(self.tstamp_col)
    #         if isinstance(tstamp, str):
    #             tstamp = datetime.fromisoformat(tstamp)
    #         elif tstamp is None:
    #             tstamp = datetime.utcnow()
    #         if not tstamp.tzinfo:
    #             tstamp = tstamp.replace(tzinfo=ZoneInfo("UTC"))
    #         self.data["_created_at"] = tstamp

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def is_from_cdc(self) -> bool:
        """If `True` CDC source is used to build the table"""
        if self.source is None:
            return False
        else:
            return self.source.is_cdc

    @property
    def layer_spark_chain(self):
        nodes = []

        if self.layer == "BRONZE":
            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_bronze_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    ),
                ]

        elif self.layer == "SILVER":
            if self.timestamp_key:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_tstamp",
                            "type": "timestamp",
                        },
                        sql_expression=self.timestamp_key,
                    )
                ]

            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_silver_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    )
                ]

        elif self.layer == "GOLD":
            if self.add_laktory_columns:
                nodes += [
                    SparkChainNode(
                        column={
                            "name": "_gold_at",
                            "type": "timestamp",
                        },
                        spark_func_name="current_timestamp",
                    )
                ]

        if self.drop_duplicates:
            subset = None
            if isinstance(self.drop_duplicates, list):
                subset = self.drop_duplicates
            elif self.primary_key:
                subset = [self.primary_key]

            nodes += [
                SparkChainNode(
                    spark_func_name="dropDuplicates", spark_func_args=[subset]
                )
            ]

        if self.drop_source_columns:
            nodes += [
                SparkChainNode(
                    spark_func_name="drop",
                    spark_func_args=[
                        c
                        for c in self.spark_chain.columns[0]
                        if c not in ["_bronze_at", "_silver_at", "_gold_at"]
                    ],
                )
            ]

        if len(nodes) == 0:
            return None

        return SparkChain(nodes=nodes)
