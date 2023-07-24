from laktory._logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Readers                                                                     #
# --------------------------------------------------------------------------- #

def read(*args, database=None, **kwargs):
    try:
        import dlt
        return dlt.read(*args, **kwargs)
    except (ModuleNotFoundError, FileNotFoundError):
        table_name = args[0]
        if database is not None:
            table_name = f"{database}.{table_name}"
        return spark.read.table(table_name)


def read_stream(*args, database=None, fmt="delta", **kwargs):
    try:
        import dlt
        return dlt.read_stream(*args, **kwargs)
    except (ModuleNotFoundError, FileNotFoundError):
        table_name = args[0]
        if database is not None:
            table_name = f"{database}.{table_name}"
        return spark.readStream.format(fmt).table(table_name)


# --------------------------------------------------------------------------- #
# Decorators                                                                  #
# --------------------------------------------------------------------------- #

try:
    import dlt

    table = dlt.table
    view = dlt.view
    expect = dlt.expect
    expect_or_drop = dlt.expect_or_drop
    expect_or_fail = dlt.expect_or_fail
    expect_all = dlt.expect_all
    expect_all_or_drop = dlt.expect_all_or_drop
    expect_all_or_fail = dlt.expect_all_or_fail

except (ModuleNotFoundError, FileNotFoundError):

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


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def write_sdf(sdf, table_name, database=None):

    logger.info(f"Writing temporary table")
    if database is not None:
        table_name = f"{database}.{table_name}"

    sdf \
        .write \
        .mode("overwrite") \
        .format("parquet") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
