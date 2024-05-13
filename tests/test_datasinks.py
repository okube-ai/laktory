import os
import sys
import shutil
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

from laktory.models import TableDataSink
from laktory.models import FileDataSink
from laktory._testing import df_slv
from laktory._testing import Paths
from laktory._testing import spark

paths = Paths(__file__)


#
#
# import os
# import pandas as pd
#
# from laktory.models.datasources import FileDataSource
# from laktory.models.datasources import MemoryDataSource
# from laktory.models.datasources import TableDataSource
# from laktory._testing import Paths
# from laktory._testing import spark
#
# paths = Paths(__file__)
#
# # DataFrame
# pdf = pd.DataFrame(
#     {
#         "x": [1, 2, 3],
#         "a": [1, -1, 1],
#         "b": [2, 0, 2],
#         "c": [3, 0, 3],
#         "n": [4, 0, 4],
#     },
# )
# df0 = spark.createDataFrame(pdf)


def test_file_data_sink():

    dirpath = os.path.join(paths.tmp, "df_slv_sink")
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)

    # Write as overwrite
    source = FileDataSink(
        path=dirpath,
        format="PARQUET",
        mode="overwrite",
    )
    source.write(df_slv)

    # Write as append
    source.write(df_slv, mode="append")

    # Write and raise error
    with pytest.raises(AnalysisException):
        source.write(df_slv, mode="error")

    # Read back
    df = spark.read.format("PARQUET").load(dirpath)

    # Test
    assert df.count() == 2 * df_slv.count()

    # Cleanup
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)


if __name__ == "__main__":
    test_file_data_sink()
