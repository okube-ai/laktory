import os
import shutil
from pathlib import Path
from typing import Union
from typing import Any
from typing import Literal
from pydantic import Field
from laktory._logger import get_logger
from laktory.models.basemodel import BaseModel
from laktory.spark import is_spark_dataframe
from laktory.spark import SparkDataFrame
from laktory.polars import is_polars_dataframe
from laktory.polars import PolarsLazyFrame
from laktory.types import AnyDataFrame

logger = get_logger(__name__)


class BaseDataSink(BaseModel):
    """
    Base class for building data sink

    Attributes
    ----------
    from_quarantine:
        Only includes quarantined results based on node expectations.
    primary:
        A primary sink will be used to read data for downstream nodes when
        moving from stream to batch. Don't apply for quarantine sinks.
    mode:
        Write mode.
        - overwrite: Overwrite existing data
        - append: Append contents of the dataframe to existing data
        - error: Throw and exception if data already exists
        - ignore: Silently ignore this operation if data already exists
        - complete: Overwrite for streaming dataframes
    write_options:
        Other options passed to `spark.write.options`
    """

    checkpoint_location: str = None
    from_quarantine: bool = False
    mode: Union[
        Literal["OVERWRITE", "APPEND", "IGNORE", "ERROR", "COMPLETE", "UPDATE"], None
    ] = None
    primary: bool = True
    write_options: dict[str, str] = {}
    _parent: "PipelineNode" = None

    # ----------------------------------------------------------------------- #
    # Properties                                                              #
    # ----------------------------------------------------------------------- #

    @property
    def _id(self):
        return str(self)

    @property
    def _checkpoint_location(self) -> Path:

        if self.checkpoint_location:
            return Path(self.checkpoint_location)

        if self._parent and self._parent._root_path:
            for i, s in enumerate(self._parent.all_sinks):
                if s == self:
                    return self._parent._root_path / f"sink-{i:03d}" / "checkpoint"

        return None

    # ----------------------------------------------------------------------- #
    # Writers                                                                 #
    # ----------------------------------------------------------------------- #

    def write(self, df: AnyDataFrame, mode=None) -> None:
        """
        Write dataframe into sink.

        Parameters
        ----------
        df:
            Input dataframe
        mode:
            Write mode overwrite of the sink default mode.

        Returns
        -------
        """
        if mode is None:
            mode = self.mode
        if is_spark_dataframe(df):
            self._write_spark(df, mode=mode)
        elif is_polars_dataframe(df):
            self._write_polars(df, mode=mode)
        else:
            raise ValueError(f"DataFrame type '{type(df)}' not supported")

    def _write_spark(self, df: SparkDataFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Spark dataframe")

    def _write_polars(self, df: PolarsLazyFrame, mode=mode) -> None:
        raise NotImplementedError("Not implemented for Polars dataframe")

    # ----------------------------------------------------------------------- #
    # Purge                                                                   #
    # ----------------------------------------------------------------------- #

    def _purge_checkpoint(self, spark=None):
        if self._checkpoint_location:
            if os.path.exists(self._checkpoint_location):
                logger.info(
                    f"Deleting checkpoint at {self._checkpoint_location}",
                )
                shutil.rmtree(self._checkpoint_location)

            if spark is None:
                return

            try:
                from pyspark.dbutils import DBUtils
            except ModuleNotFoundError:
                return

            dbutils = DBUtils(spark)

            _path = self._checkpoint_location.as_posix()
            _path = self._checkpoint_location
            try:
                dbutils.fs.ls(
                    _path
                )  # TODO: Figure out why this does not work with databricks connect
                logger.info(
                    f"Deleting checkpoint at dbfs {_path}",
                )
                dbutils.fs.rm(_path, True)

            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    pass
                elif "databricks.sdk.errors.platform.ResourceDoesNotExist" in str(
                    type(e)
                ):
                    pass
                elif "databricks.sdk.errors.platform.InvalidParameterValue" in str(
                    type(e)
                ):
                    # TODO: Figure out why this is happening. It seems that the databricks SDK
                    #       modify the path before sending to REST API.
                    logger.warn(f"dbutils could not delete checkpoint {_path}: {e}")
                else:
                    raise e

    def purge(self):
        """
        Delete sink data and checkpoints
        """
        raise NotImplementedError()

    # ----------------------------------------------------------------------- #
    # Sources                                                                 #
    # ----------------------------------------------------------------------- #

    def as_source(self, as_stream=None):
        raise NotImplementedError()

    def read(self, spark=None, as_stream=None):
        """
        Read dataframe from sink.

        Parameters
        ----------
        spark:
            Spark Session
        as_stream:
            If `True`, dataframe read as stream.

        Returns
        -------
        """
        return self.as_source(as_stream=as_stream).read(spark=spark)
