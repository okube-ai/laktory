from . import api
from . import pipeline
from functools import partial

def _create_dlt_table_fn():
    """
    Wraps spark.table function, so that referring to live tables is handled by DLT.
    """
    # Original spark.table function
    spark_table = pipeline.instance._spark.__class__.table
    LIVE_VIRTUAL_SCHEMA = "live."

    # We will patch spark.table with this function
    def dlt_table_fn(self, name):
        if name.lower().startswith(LIVE_VIRTUAL_SCHEMA):
            return api.read(name[len(LIVE_VIRTUAL_SCHEMA):])
        else:
            return spark_table(self, name)
    return dlt_table_fn

def _create_dlt_read_table_fn():
    """
    Wraps spark.read function, so that referring to live tables is handled by DLT.
    """
    # Original spark.read.table function
    spark_read_table = pipeline.instance._spark.read.__class__.table
    LIVE_VIRTUAL_SCHEMA = "live."

    # We will patch spark.table with this function
    def dlt_read_table_fn(self, name):
        if name.lower().startswith(LIVE_VIRTUAL_SCHEMA):
            return api.read(name[len(LIVE_VIRTUAL_SCHEMA):])
        else:
            return spark_read_table(self, name)
    return dlt_read_table_fn

def _create_dlt_streaming_table_fn():
    """
    Wraps spark.readStream function, so that referring to live tables is handled by DLT.
    """
    # Original spark.readStream.table function
    spark_read_stream_table = pipeline.instance._spark.readStream.__class__.table
    LIVE_VIRTUAL_SCHEMA = "live."

    # We will patch spark.readStream.table with this function
    def dlt_read_stream_table_fn(self, name):
        if name.lower().startswith(LIVE_VIRTUAL_SCHEMA):
            return api.read_stream_with_reader(name[len(LIVE_VIRTUAL_SCHEMA):], self)
        else:
            return spark_read_stream_table(self, name)
    return dlt_read_stream_table_fn


def _override_spark_functions(block_unsupported_sql_query_plans = True):
    """
    Wraps spark table/read/readStream/sql functions, so that referring to live tables
    is handled by DLT
    :param block_unsupported_sql_query_plans: Whether to block SQL query plans not supported by DLT.
    """
    spark = pipeline.instance._spark
    spark.__class__.table = _create_dlt_table_fn()
    spark.read.__class__.table = _create_dlt_read_table_fn()
    spark.readStream.__class__.table = _create_dlt_streaming_table_fn()

    def _dlt_sql_fn1(self, stmt):
        return api._sql(stmt, True)
    def _dlt_sql_fn2(self, stmt):
        return api._sql(stmt, False)

    if (block_unsupported_sql_query_plans):
        spark.__class__.sql =  _dlt_sql_fn1
    else:
        # This is used in the context of notebook development experience.
        spark.__class__.sql = _dlt_sql_fn2
