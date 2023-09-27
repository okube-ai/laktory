from laktory._logger import get_logger

logger = get_logger(__name__)

dlt_available = False
try:
    from dlt import *
    from dlt import read as _read
    from dlt import read_stream as _read_stream
    dlt_available = True
except Exception:
    pass


# --------------------------------------------------------------------------- #
# Utilities                                                                   #
# --------------------------------------------------------------------------- #

dbr_version = spark.conf.get("pipelines.dbrVersion", None)


def is_debug():
    try:
        import dlt
    except Exception:
        return True
    if dbr_version is None:
        return True
    return False


# --------------------------------------------------------------------------- #
# Overwrite                                                                   #
# --------------------------------------------------------------------------- #

def read(*args, **kwargs):
    if is_debug():
        return spark.read.table(args[0])
    else:
        # Remove catalog and database from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read(*args, **kwargs)


def read_stream(*args, fmt="delta", **kwargs):
    if is_debug():
        return spark.readStream.format(fmt).table(args[0])
    else:
        # Remove catalog and database from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read_stream(*args, **kwargs)


# --------------------------------------------------------------------------- #
# Mocks                                                                       #
# --------------------------------------------------------------------------- #

if not dlt_available:
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

