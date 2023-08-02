try:
    # Import databricks dlt module, only available for DBR >= 13
    from dlt import *
    from dlt import read as _read
except (ModuleNotFoundError, FileNotFoundError):
    try:
        # Import local copy for dlt for DBR < 13
        from laktory._databricks_dlt import *
    except RuntimeError:
        # Fail if spark context not available
        pass


# --------------------------------------------------------------------------- #
# Utilities                                                                   #
# --------------------------------------------------------------------------- #

def is_pipeline(spark):
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


def read(*args, spark=None, **kwargs):
    if is_pipeline(spark):
        # Remove catalog and database from naming space
        args = list(args)
        args[0] = args[0].split(".")[-1]
        return _read(*args, **kwargs)
    else:
        return spark.read.table(args[0])