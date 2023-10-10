from laktory._logger import get_logger
from laktory.spark import DataFrame

try:
    from dlt import *
    from dlt import read as _read
    from dlt import read_stream as _read_stream
except Exception:
    pass


logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Utilities                                                                   #
# --------------------------------------------------------------------------- #


def is_mocked() -> bool:
    """
    DLT module mock flag. If True, native Databricks DLT functions are mocked
    to allow debugging by running a pipeline notebook against an arbitrary
    cluster even when the native DLT module is not available.

    DLT is mocked in the following situations:
        - notebook ran outside a DLT pipeline and DBR < 13
        - notebook ran outside a DLT pipeline and DBR >= 13 with shared access
          mode cluster
    """
    try:
        import dlt

        return False
    except Exception:
        return True


def is_debug() -> bool:
    """
    Debug flag. If True, DLT readers are replaced with laktory functions
    allowing to run a notebook outside of a pipeline and preview the content
    of the output DataFrame.
    """
    try:
        import dlt
    except Exception:
        return True
    if spark.conf.get("pipelines.dbrVersion", None) is None:
        return True
    return False


def get_df(df_wrapper) -> DataFrame:
    """
    When executed in debug mode (ref `dlt.is_debug`), executes and returns
    the output DataFrame of a function decorated with `@dlt.table` or
    `@dlt.view`. Returns None when `is_debug()` is `False`.

    This method is not supported when a table using DLT views as input.
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


def read(*args, **kwargs):
    if is_debug():
        return spark.read.table(args[0])
    else:
        # Remove catalog and schema from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read(*args, **kwargs)


def read_stream(*args, fmt="delta", **kwargs):
    if is_debug():
        return spark.readStream.format(fmt).table(args[0])
    else:
        # Remove catalog and schema from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read_stream(*args, **kwargs)


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
