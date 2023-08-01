try:
    # Import databricks dlt module, only available for DBR >= 13
    from dlt import *
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
