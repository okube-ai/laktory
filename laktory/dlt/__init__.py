try:
    # Import databricks dlt module, only available for DBR >= 13
    from dlt import *
    from dlt import read as _read
    from dlt import read_stream as _read_stream
except (ModuleNotFoundError, FileNotFoundError):
    raise ModuleNotFoundError("dlt module requires a cluster with Databricks Runtime >= 13.* or to be run from Delta Live Tables")


# --------------------------------------------------------------------------- #
# Utilities                                                                   #
# --------------------------------------------------------------------------- #

def is_pipeline():
    try:
        import dlt
    except (ModuleNotFoundError, FileNotFoundError):
        return False
    dbr_version = spark.conf.get("pipelines.dbrVersion", None)
    if dbr_version is None:
        return False
    return True


# --------------------------------------------------------------------------- #
# Overwrite                                                                   #
# --------------------------------------------------------------------------- #

def read(*args, **kwargs):
    if is_pipeline():
        # Remove catalog and database from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read(*args, **kwargs)
    else:
        return spark.read.table(args[0])


def read_stream(*args, fmt="delta", **kwargs):
    if is_pipeline():
        # Remove catalog and database from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read_stream(*args, **kwargs)
    else:
        return spark.readStream.format(fmt).table(args[0])
