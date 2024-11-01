from laktory._logger import get_logger
from laktory.spark import SparkDataFrame

try:
    from dlt import *
    from dlt import read as _read
    from dlt import read_stream as _read_stream
    from dlt import apply_changes as _apply_changes
except Exception:
    pass


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Utilities                                                                   #
# --------------------------------------------------------------------------- #


def is_mocked() -> bool:
    """
    DLT module mock flag. If `True`, native Databricks DLT functions are mocked
    to allow debugging by running a pipeline notebook against an arbitrary
    cluster even when the native DLT module is not available.

    DLT is mocked in the following situations:

    * notebook ran outside a DLT pipeline and DBR < 13
    * notebook ran outside a DLT pipeline and DBR >= 13 with shared access
    mode cluster

    Returns
    -------
    :
        Mocked flag
    """
    try:
        import dlt

        return False
    except Exception:
        return True


def is_debug() -> bool:
    """
    Debug flag. If `True`, DLT readers are replaced with laktory functions
    allowing to run a notebook outside of a pipeline and preview the content
    of the output dataframe.

    Returns
    -------
    :
        Debug flag
    """
    try:
        import dlt
    except Exception:
        return True
    if spark.conf.get("pipelines.dbrVersion", None) is None:
        return True
    return False


def get_df(df_wrapper) -> SparkDataFrame:
    """
    When executed in debug mode (ref `dlt.is_debug`), executes and returns
    the output dataframe of a function decorated with `@dlt.table` or
    `@dlt.view`. Returns None when `is_debug()` is `False`.

    This method is not supported when a table using DLT views as input.

    Returns
    -------
    :
        Output dataframe

    Examples
    --------
    ```py
    from laktory import dlt

    dlt.spark = spark

    def define_table():
        @dlt.table(name="stock_prices")
        def get_df():
            df = spark.createDataFrame([[1.0, 1.1, 0.98]])
            return df

        return get_df

    wrapper = define_table()
    df = dlt.get_df(wrapper)
    display(df)
    ```
    """
    df = None
    if is_debug():
        if is_mocked():
            df = df_wrapper()
        else:
            df = df_wrapper().func()
    return df


# --------------------------------------------------------------------------- #
# Overwrite                                                                   #
# --------------------------------------------------------------------------- #


def read(*args, **kwargs) -> SparkDataFrame:
    """
    When `is_debug()` is `True` read table from storage, else read table from
    pipeline with native Databricks `dlt.read`

    Returns
    -------
    :
        Ouput dataframe

    Examples
    --------
    ```py
    from laktory import dlt

    dlt.spark = spark

    def define_table():
        @dlt.table(name="slv_stock_prices")
        def get_df():
            df = dlt.read("dev.finance.brz_stock_prices")
            return df

        return get_df

    define_table()
    ```
    """
    if is_debug():
        return spark.read.table(args[0])
    else:
        # Remove catalog and schema from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read(*args, **kwargs)


def read_stream(*args, fmt="delta", **kwargs):
    """
    When `is_debug()` is `True` read table from storage as stream, else read
    table from pipeline with native Databricks `dlt.read_stream`

    Returns
    -------
    :
        Ouput dataframe

    Examples
    --------
    ```py
    from laktory import dlt

    dlt.spark = spark

    def define_table():
        @dlt.table(name="slv_stock_prices")
        def get_df():
            df = dlt.read_stream("dev.finance.brz_stock_prices")
            return df

        return get_df

    define_table()
    ```
    """

    if is_debug():
        return spark.readStream.format(fmt).table(args[0])
    else:
        # Remove catalog and schema from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read_stream(*args, **kwargs)


def apply_changes(*args, node=None, **kwargs):
    """
    When `is_debug()` is `True` read source CDC table from storage, else run
    native Databricks `dlt.apply_changes`

    Returns
    -------
    :
        Output dataframe

    Examples
    --------
    ```py
    from laktory import dlt
    from laktory import models

    dlt.spark = spark

    def define_table(node):
        dlt.create_streaming_table(name=node.name)
        df = dlt.apply_changes(**node.apply_changes_kwargs)
        return df

    define_table(
        models.PipelineNode(
            name="slv_stock_prices",
            source={
                "table_name": "brz_stock_prices",
                "cdc": {
                    "primary_keys": ["asset_symbol"],
                    "sequence_by": "change_id",
                    "scd_type": 2,
                },
            },
            sinks=[
                {
                    "table_name": "brz_stock_prices",
                }
            ],
        )
    )
    ```
    """
    if is_debug():
        if node is None:
            return
        df = node.source.read(spark=spark)
        # TODO: Apply changes
        logger.warning(
            "Laktory does not currently support applying CDC changes. Returned dataframe is CDC source."
        )
        return df
    else:
        return _apply_changes(*args, **kwargs)


# --------------------------------------------------------------------------- #
# Mocks                                                                       #
# --------------------------------------------------------------------------- #

if is_mocked():

    def table(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                logger.info(f"Running {func.__name__}")
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def view(*view_args, **view_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                logger.info(f"Running {func.__name__}")
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect_or_drop(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect_or_fail(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect_all(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect_all_or_drop(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def expect_all_or_fail(*table_args, **table_kwargs):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    def create_streaming_table(*args, **kwargs):
        pass


# --------------------------------------------------------------------------- #
# Wrappers                                                                    #
# --------------------------------------------------------------------------- #


def table_or_view(*args, as_view=False, **kwargs):
    if as_view:
        return view(*args, **kwargs)
    else:
        return table(*args, **kwargs)
